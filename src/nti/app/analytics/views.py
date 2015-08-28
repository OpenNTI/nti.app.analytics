#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from datetime import datetime

from zope.event import notify
from zope.schema.interfaces import ValidationError

from ZODB.interfaces import IBroken

from pyramid.view import view_config
from pyramid import httpexceptions as hexc

from nti.app.base.abstract_views import AbstractAuthenticatedView
from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.analytics.database import get_analytics_db
from nti.analytics.database.sessions import Location
from nti.analytics.database.sessions import IpGeoLocation

from nti.analytics.model import delete_research_status
from nti.analytics.model import UserResearchStatusEvent
from nti.analytics.model import AnalyticsClientParams

from nti.analytics.sessions import handle_new_session
from nti.analytics.sessions import handle_end_session
from nti.analytics.sessions import update_session

from nti.analytics.resource_views import handle_events
from nti.analytics.resource_views import get_progress_for_ntiid
from nti.analytics.resource_views import get_video_progress_for_course

from nti.analytics.interfaces import IBatchResourceEvents
from nti.analytics.interfaces import IAnalyticsSessions
from nti.analytics.interfaces import IUserResearchStatus

from nti.analytics.progress import get_assessment_progresses_for_course

from nti.common.maps import CaseInsensitiveDict

from nti.contenttypes.courses.interfaces import ICourseInstance
from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode

from nti.contentlibrary.indexed_data import get_catalog

from nti.dataserver.users import User
from nti.dataserver.interfaces import IUser
from nti.dataserver import authorization as nauth
from nti.dataserver.interfaces import IEnumerableEntityContainer

from nti.externalization import internalization
from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.externalization import to_external_object

from nti.ntiids import ntiids

from . import BATCH_EVENTS
from . import SYNC_PARAMS
from . import ANALYTICS_SESSION
from . import END_ANALYTICS_SESSION
from . import ANALYTICS_SESSIONS

ALL_USERS = 'ALL_USERS'
SET_RESEARCH_VIEW = 'SetUserResearch'
GEO_LOCATION_VIEW = 'GetGeoLocations'

def _is_true(t):
	result = bool(t and str(t).lower() in ('1', 'y', 'yes', 't', 'true'))
	return result

def _get_last_mod( progress, max_last_mod ):
	"For progress, get the most recent date as our last modified."
	result = max_last_mod

	if 		not max_last_mod \
		or 	( 	progress.last_modified and \
				progress.last_modified > max_last_mod ):
		result = progress.last_modified
	return result

def _process_batch_events( events ):
	"Process the events, returning a tuple of events queued and malformed events."
	batch_events = []
	malformed_count = 0

	# Lets hand-internalize these objects one-by-one so that we
	# can exclude any malformed objects and process the proper events.
	for event in events:
		factory = internalization.find_factory_for(event)
		new_event = factory()
		try:
			internalization.update_from_external_object(new_event, event)
			batch_events.append( new_event )
		except ValidationError as e:
			# The app may resend events if we err; so we should just log.
			logger.warn('Malformed events received (event=%s) (%s)', event, e)
			malformed_count += 1

	event_count = handle_events( batch_events )
	return event_count, malformed_count

@view_config(route_name='objects.generic.traversal',
			 name=BATCH_EVENTS,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class BatchEvents(	AbstractAuthenticatedView,
					ModeledContentUploadRequestUtilsMixin ):
	"""
	A view that accepts a batch of analytics events.  The view
	will parse the input and process the events (e.g. queueing).
	"""

	content_predicate = IBatchResourceEvents.providedBy

	def _do_call(self):
		external_input = self.readInput()
		events = external_input['events']
		total_count = len(events)

		event_count, malformed_count = _process_batch_events( events )
		logger.info('Received batched analytic events (count=%s) (total_count=%s) (malformed=%s)',
					event_count, total_count, malformed_count )

		result = LocatedExternalDict()
		result['EventCount'] = event_count
		result['MalformedEventCount'] = malformed_count
		return result

@view_config(route_name='objects.generic.traversal',
			 name=SYNC_PARAMS,
			 renderer='rest',
			 request_method='GET')
class BatchEventParams( AbstractAuthenticatedView ):

	def __call__(self):
		# Return our default analytic client params
		client_params = AnalyticsClientParams()
		return client_params

@view_config(route_name='objects.generic.traversal',
			 name=ANALYTICS_SESSION,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class AnalyticsSession( AbstractAuthenticatedView ):

	def __call__(self):
		"""
		Create a new analytics session and place it in a cookie.
		"""
		request = self.request
		user = request.remote_user
		if user is not None:
			handle_new_session(user, request)
		return request.response

@view_config(route_name='objects.generic.traversal',
			 name=END_ANALYTICS_SESSION,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class EndAnalyticsSession(AbstractAuthenticatedView, ModeledContentUploadRequestUtilsMixin):
	"""
	Ends an analytic session, defined by information in the
	header or cookie of this request.  Optionally accepts a
	`timestamp` param, allowing the client to specify the
	session end time.

	timestamp
		The (optional) seconds since the epoch marking when
		the session ended.

	batch_events
		The (optional) closed batch_events, occurring at the end of session.
	"""

	def __call__(self):
		"""
		End the current analytics session.
		"""
		request = self.request
		user = request.remote_user

		values = CaseInsensitiveDict(self.readInput())
		timestamp = values.get( 'timestamp' )
		batch_events = values.get( 'batch_events' )

		if batch_events:
			events = batch_events.get( 'events' )

			if events:
				total_count = len( events )
				event_count, malformed_count = _process_batch_events( events )
				logger.info('Process batched analytic events on session close (count=%s) (total_count=%s) (malformed=%s)',
						event_count, total_count, malformed_count )

		handle_end_session( user, request, timestamp=timestamp )
		return hexc.HTTPNoContent()

@view_config(route_name='objects.generic.traversal',
			 name=ANALYTICS_SESSIONS,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class UpdateAnalyticsSessions(AbstractAuthenticatedView,
							  ModeledContentUploadRequestUtilsMixin):

	content_predicate = IAnalyticsSessions.providedBy

	def __call__(self):
		"""
		Will accept one or many IAnalyticsSession objects, which we will synchronously
		resolve the session_id for before returning.  If there is already a session_id,
		we'll update the state (end_time) of the given session object.
		"""
		request = self.request
		user = request.remote_user

		external_input = self.readInput()
		factory = internalization.find_factory_for( external_input )
		sessions = factory()
		internalization.update_from_external_object( sessions, external_input )

		ip_addr = getattr( request, 'remote_addr' , None )
		user_agent = getattr( request, 'user_agent', None )

		results = []
		for session in sessions.sessions:
			try:
				result = update_session( session, user, user_agent=user_agent, ip_addr=ip_addr )
				results.append( result )
			except ValueError as e:
				# Append invalid session information.  We still return a 200 though.
				val = dict()
				val['Error'] = e.message
				results.append( val )
		return results

def _get_children_ntiid_legacy( unit, accum ):
	for attr in ('ntiid', 'target_ntiid'):
		ntiid_val = getattr( unit, attr, None )
		if ntiid_val is not None:
			accum.add( ntiid_val )
	for ntiid in unit.embeddedContainerNTIIDs:
		accum.add( ntiid )
	for child in unit.children:
		_get_children_ntiid_legacy( child, accum )

def _get_children_ntiid( unit ):
	catalog = get_catalog()
	rs = catalog.search_objects( container_ntiids=unit.ntiid )
	contained_objects = tuple( rs )
	results = set()
	if not contained_objects:
		# Probably a unit from a global, non-persistent course;
		# iterating is the best we can do.
		_get_children_ntiid_legacy( unit, results )
	else:
		for contained_object in contained_objects:
			for attr in ('ntiid', 'target_ntiid'):
				ntiid_val = getattr( contained_object, attr, None )
				if ntiid_val is not None:
					results.add( ntiid_val )
	return results

@view_config(route_name='objects.generic.traversal',
			 renderer='rest',
			 context=ICourseOutlineContentNode,
			 request_method='GET',
			 permission=nauth.ACT_READ,
			 name="Progress" )
class CourseOutlineNodeProgress( AbstractAuthenticatedView,
								 ModeledContentUploadRequestUtilsMixin):
	"""
	For the given content outline node, return the progress we have for the user
	on each ntiid within the content node.  This will include self-assessments and
	assignments for the course.  On return, the 'LastModified' header will be set, allowing
	the client to specify the 'If-Modified-Since' header for future requests.  A 304 will be
	returned if there is the results have not changed.
	"""

	def __call__(self):
		# - Locally, this is quick. ~1s (much less when cached) to get
		# ntiids under node; ~.05s to get empty resource set.  Bumps up to ~.3s
		# once the user starts accumulating events.
		user = self.getRemoteUser()
		ntiid = self.context.ContentNTIID
		content_unit = ntiids.find_object_with_ntiid( ntiid )
		node_ntiids = _get_children_ntiid( content_unit )

		result = LocatedExternalDict()
		result[StandardExternalFields.CLASS] = 'CourseOutlineNodeProgress'
		result[StandardExternalFields.MIMETYPE] = 'application/vnd.nextthought.progresscontainer'
		result[StandardExternalFields.ITEMS] = item_dict = {}

		node_last_modified = None

		# Get progress for resource/videos
		for node_ntiid in node_ntiids:
			# Can improve this if we can distinguish between video and other.
			node_progress = get_progress_for_ntiid( user, node_ntiid )

			if node_progress:
				item_dict[node_ntiid] = to_external_object( node_progress )
				node_last_modified = _get_last_mod( node_progress, node_last_modified )

		# Get progress for self-assessments and assignments
		try:
			course = ICourseInstance( content_unit )
		except TypeError:
			logger.warn( 'No course found for content unit; cannot return progress for assessments (%s)',
						ntiid )
			course = None

		if course is not None:
			# Gathering all assignments/self-assessments for course.
			# May be cheaper than finding just for our unit.
			progresses = get_assessment_progresses_for_course( user, course )
			for progress in progresses:
				item_dict[progress.progress_id] = to_external_object( progress )
				node_last_modified = _get_last_mod( progress, node_last_modified )

		# We could summarize progress for node. This might be difficult unless we assume
		# that every child ntiid contributes towards progress.  If we need to filter
		# out certain types of ntiids, that might be tough.

		# Setting this will enable the renderer to return a 304, if needed.
		self.request.response.last_modified = node_last_modified
		return result

@view_config(route_name='objects.generic.traversal',
			 renderer='rest',
			 context=ICourseInstance,
			 request_method='GET',
			 permission=nauth.ACT_READ,
			 name="VideoProgress" )
class UserCourseVideoProgress(AbstractAuthenticatedView,
							  ModeledContentUploadRequestUtilsMixin):
	"""
	For the given course instance, return the progress we have for the user
	on each video in the course.

	On return, the 'LastModified' header will be set, allowing
	the client to specify the 'If-Modified-Since' header for future requests.  A 304 will be
	returned if there is the results have not changed.
	"""

	def __call__(self):
		user = self.getRemoteUser()
		course = self.context

		result = LocatedExternalDict()
		result[StandardExternalFields.CLASS] = 'CourseVideoProgress'
		result[StandardExternalFields.ITEMS] = item_dict = {}
		node_last_modified = None

		video_progress_col = get_video_progress_for_course( user, course )

		for video_progress in video_progress_col:
			item_dict[video_progress.ResourceID] = to_external_object( video_progress )
			node_last_modified = _get_last_mod( video_progress, node_last_modified )

		# Setting this will enable the renderer to return a 304, if needed.
		self.request.response.last_modified = node_last_modified
		return result

@view_config( route_name='objects.generic.traversal',
			  renderer='rest',
			  context=IUser,
			  request_method='POST',
			  name=SET_RESEARCH_VIEW)
class UserResearchStudyView(AbstractAuthenticatedView,
							ModeledContentUploadRequestUtilsMixin ):
	"""
	Updates a user's research status.
	"""

	def __call__(self):
		values = CaseInsensitiveDict(self.readInput())
		allow_research = values.get('allow_research')
		allow_research = _is_true(allow_research)
		user = self.request.context

		research_status = IUserResearchStatus(user)
		if IBroken.providedBy(research_status):
			delete_research_status(user)
			research_status = IUserResearchStatus(user)
		research_status.updateLastMod()
		research_status.modified = datetime.utcnow()
		research_status.allow_research = allow_research

		logger.info('Setting research status for user (user=%s) (allow_research=%s)',
					user.username, allow_research )

		notify(UserResearchStatusEvent(user, allow_research))
		return hexc.HTTPNoContent()
	

@view_config( route_name='objects.generic.traversal',
			  renderer='rest',
			  context=ICourseInstance,
			  request_method='GET',
			  name=GEO_LOCATION_VIEW)
class UserLocationView(AbstractAuthenticatedView):
	"""
	Provides a representation of the geographical 
	locations of users within a course.
	"""
	def _get_location_ids_for_users( self, db, user_ids ): 
	    location_ids = []
	    for user_id in user_ids:
	        ips_for_user = db.session.query( IpGeoLocation ).filter( IpGeoLocation.user_id == user_id).all()
	        for row in ips_for_user:
        		location_ids.append( row.location_id )
	    return location_ids
	
	def _get_locations_for_ids( self, db, location_ids ):
	    location_rows = []
	    for location_id in location_ids:
	        location = db.session.query( Location ).filter( Location.location_id == location_id ).first()
	        # count the number of times this location is being used
 	      	if location_id in self.location_counts:
 	      		self.location_counts[location_id] += 1
 	       	else: 
 	       		# return a list of distinct locations,
 	       		# so only add each location once
		        location_rows.append(location)
 	       		self.location_counts[location_id] = 1
	    return location_rows
	
	def _get_location_data( self, locations ):
	
	    def get_user_label( number_of_users ):
	        if number_of_users > 1:
	            return '%s users' % number_of_users
	        else:
	            return '1 user'
	    
	    db_data = []
	    
	    for location in locations:
	
	        city = location.city
	        state = location.state
	        country = location.country
	
	        if country == 'United States of America':
	            if city and state:
	                label =  "%s, %s" % ( city, state )
	            elif state:
	                label =  "%s" % state
	            else:
	                label = ''
	        else:
	            if city and country:
	                label =  "%s, %s" % ( city, country )
	            elif country:
	                label =  "%s" % country
	            else:
	                label = ''
	
	        label = '%s (%s)' % (label, get_user_label(self.location_counts[location.location_id]))
	
	        number_of_users_in_location = self.location_counts[location.location_id]
	
	        locationData = {
						'latitude': float( location.latitude ), 
						'longitude': float( location.longitude ), 
						'label': label, 
						'number_of_students': number_of_users_in_location}
	        # append the data for this location as a location element in db_data
	        db_data.append( locationData )
	        
	    return db_data
	
	def get_json_data(self, course, enrollment_scope):

		db = get_analytics_db()
		locationsTable = Location
		self.location_counts = {}
		
		user_ids = _get_enrolled_user_ids( course, enrollment_scope )
		location_ids = self._get_location_ids_for_users(db, user_ids)
		locations = self._get_locations_for_ids( db, location_ids )
		data = self._get_location_data( locations )
		
		return data
	
	def get_html_view(self, course, enrollment_scope):
		
		location_data = self.get_json_data(course, enrollment_scope)
		
		output_data = []
		output_data.append( [ 'Lat', 'Long', 'Label' ] )
		
		for location in location_data:
			output_data.append([location['latitude'], 
								location['longitude'], 
								location['label']])
			
		output = """
                <html>
                    <head>
                        <script type="text/javascript" src="https://www.google.com/jsapi"></script>
                        <script>
                        google.load('visualization', '1', { 'packages': ['map'] });
                        google.setOnLoadCallback(drawMap);

                        function drawMap() {
                          var data = google.visualization.arrayToDataTable( %s );

                        var options = { showTip: true };

                        var map = new google.visualization.Map(document.getElementById('chart_div'));

                        map.draw(data, options);
                        };
                        </script>
                    </head>
                    <body>
                        <div id="chart_div" style="width: 1400px; height: 700px"></div>
                    </body>
                </html>
            """ \
            % output_data
		
	def __call__(self):
		
		return to_external_object( self.get_json_data(self.context, ALL_USERS) )
	
def _get_enrolled_user_ids(self, course, enrollment_scope):
	"""
	Gets a list of the user ids for the specified course and enrollment scope.
	"""
	users = _get_enrollment_scope_list(course)
	user_ids = []
	for user in users[enrollment_scope]:
		user_ids.append(user.user_id)
	return user_ids

	# TODO: Since this is nearly the same code as in the
	# __init__.py of coursewarereports/views, maybe we should
	# make a more generic method and use it both places?
def _get_enrollment_scope_list( course, instructors=set() ):
	"Build a dict of scope_name to user objects."
	results = {}
	# Lumping purchased in with public.
	public_scope = course.SharingScopes.get( 'Public', None )
	purchased_scope = course.SharingScopes.get( 'Purchased', None )
	non_public_users = set()
	for scope_name in course.SharingScopes:
		scope = course.SharingScopes.get( scope_name, None )
	
		if 		scope is not None \
			and scope not in (public_scope, purchased_scope):
			# If our scope is not 'public'-ish, store it separately.
			# All credit-type users should end up in ForCredit.
			scope_users = {x.lower() for x in IEnumerableEntityContainer(scope).iter_usernames()}
			scope_users = scope_users - instructors
			users = []
			for username in scope_users:
				user = User.get_user(username)
				if user is not None:
					users.append(user)
			results[scope_name] = users
			non_public_users = non_public_users.union( users )
	
	all_usernames = {x.lower() for x in IEnumerableEntityContainer(public_scope).iter_usernames()}
	all_users = []
	for username in all_usernames:
		user = User.get_user(username)
		if user is not None:
			users.append(user)
	public_usernames = all_usernames - non_public_users - instructors
	public_users = [] 
	for username in public_usernames:
		user = User.get_user(username)
		if user is not None:
			public_users.append(user)
	results['Public'] = public_users
	results[ALL_USERS] = all_users
	return results
		