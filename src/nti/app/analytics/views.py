#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id: views.py 122769 2017-10-04 21:56:03Z chris.utz $
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=inherit-non-class,no-self-argument,no-value-for-parameter

import csv
import time
import calendar
import datetime

from io import BytesIO

from perfmetrics import statsd_client

from pyramid import httpexceptions as hexc

from pyramid.view import view_defaults
from pyramid.view import view_config

from requests.structures import CaseInsensitiveDict

import six

from sqlalchemy import event

from zope import component
from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.component.hooks import getSite

from zope.event import notify

from zope.schema.interfaces import ValidationError

from nti.analytics.common import should_create_analytics

from nti.analytics_database.sessions import Sessions

from nti.analytics.interfaces import IAnalyticsSession
from nti.analytics.interfaces import IBatchResourceEvents
from nti.analytics.interfaces import IAnalyticsProgressEvent
from nti.analytics.interfaces import UserProcessedEventsEvent

from nti.analytics.locations import get_location_list

from nti.analytics.model import AnalyticsClientParams

from nti.analytics.progress import get_video_progress_for_course

from nti.analytics.resource_views import handle_events
from nti.analytics.resource_views import get_video_views_for_ntiid
from nti.analytics.resource_views import get_watched_segments_for_ntiid

from nti.analytics.sessions import get_user_sessions
from nti.analytics.sessions import handle_end_session
from nti.analytics.sessions import handle_new_session
from nti.analytics.sessions import get_recent_user_sessions

from nti.analytics.stats.interfaces import IActivitySource
from nti.analytics.stats.interfaces import IActiveTimesStatsSource
from nti.analytics.stats.interfaces import IActiveSessionStatsSource
from nti.analytics.stats.interfaces import IActiveUsersSource
from nti.analytics.stats.interfaces import IDailyActivityStatsSource

from nti.app.analytics import SYNC_PARAMS
from nti.app.analytics import ACTIVE_USERS
from nti.app.analytics import ANALYTICS_SESSION
from nti.app.analytics import ACTIVE_SESSION_COUNT
from nti.app.analytics import ACTIVE_TIMES_SUMMARY
from nti.app.analytics import END_ANALYTICS_SESSION
from nti.app.analytics import ACTIVITY_SUMMARY_BY_DATE
from nti.app.analytics import ANALYTICS_SESSION_COOKIE_NAME

from nti.app.analytics import MessageFactory as _

from nti.app.analytics.interfaces import IAnalyticsContext
from nti.app.analytics.interfaces import IEventsCollection
from nti.app.analytics.interfaces import IAnalyticsWorkspace
from nti.app.analytics.interfaces import ISessionsCollection

from nti.app.analytics.utils import set_research_status
from nti.app.analytics.utils import get_session_id_from_request

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import BatchingUtilsMixin
from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.renderers.caching import default_cache_controller

from nti.app.renderers.interfaces import IResponseCacheController

from nti.app.users.utils import get_user_creation_sitename, get_admins

from nti.app.products.courseware.interfaces import ICourseInstanceEnrollment

from nti.appserver.pyramid_authorization import has_permission

from nti.common.string import is_true

from nti.contenttypes.presentation.interfaces import INTIVideo

from nti.contenttypes.completion.interfaces import ICompletionContextProvider
from nti.contenttypes.completion.interfaces import UserProgressUpdatedEvent

from nti.contenttypes.courses.interfaces import ACT_VIEW_DETAILED_CONTENT_USAGE
from nti.contenttypes.courses.interfaces import ICourseInstance
from nti.contenttypes.courses.interfaces import ICourseCatalogEntry

from nti.coremetadata.interfaces import UserLastSeenEvent

from nti.dataserver import authorization as nauth

from nti.dataserver.authorization import is_admin_or_site_admin

from nti.dataserver.interfaces import IUser

from nti.dataserver.users.users import User

from nti.externalization import internalization

from nti.externalization.externalization import to_external_object

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.ntiids.ntiids import find_object_with_ntiid

from nti.traversal.traversal import find_interface

from nti.securitypolicy.utils import is_impersonating

from nti.transactions import transactions

ITEMS = StandardExternalFields.ITEMS
TOTAL = StandardExternalFields.TOTAL
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

GEO_LOCATION_VIEW = 'GeoLocations'
SET_RESEARCH_VIEW = 'SetUserResearch'

logger = __import__('logging').getLogger(__name__)


class _ICacheControlHint(interface.Interface):
    """
    A marker interface providing a hint as to how cacheable
    a response is.
    """
_ICacheControlHint.setTaggedValue('_ext_is_marker_interface', True)


class _IDailyResults(_ICacheControlHint):
    """
    A _ICacheControlHint indicating results update daily
    and as such can be cached for the current day
    """
_IDailyResults.setTaggedValue('_ext_is_marker_interface', True)


class _IHistoricalResults(_ICacheControlHint):
    """
    A _ICacheControlHint indicating results are historical
    and are unlikely to change with any frequency
    """
_IHistoricalResults.setTaggedValue('_ext_is_marker_interface', True)


def _get_last_mod(progress, max_last_mod):
    """
    For progress, get the most recent date as our last modified.
    """
    result = max_last_mod

    if     not max_last_mod \
        or (    progress.last_modified
            and progress.last_modified > max_last_mod):
        result = progress.last_modified
    return result


def notify_lastseen_event(user, request=None):
    if request is not None and not is_impersonating(request):
        notify(UserLastSeenEvent(user, time.time(), request))


def _process_batch_events(events, remote_user, request=None):
    """
    Process the events, returning a tuple of events queued and malformed events.
    """
    batch_events = []
    invalid_count = 0
    malformed_count = 0
    resource_to_root_context = ()
    remote_username = remote_user.username

    # Lets hand-internalize these objects one-by-one so that we
    # can exclude any malformed objects and process the proper events.
    for event in events:
        factory = internalization.find_factory_for(event)
        if factory is None:
            logger.warning(
                'Malformed events received (mime_type=%s) (event=%s)',
                event.get('MimeType'), event
            )
            malformed_count += 1
            continue

        new_event = factory()
        resource_to_root_context = set()
        try:
            internalization.update_from_external_object(new_event, event)
            if new_event.user != remote_username:
                # This shouldn't happen.
                logger.warning(
                    'Analytics event username does not match remote user (event=%s) (%s)',
                    new_event.user, remote_username
                )
                new_event.user = remote_username
            duration = getattr(new_event, 'Duration', None)
            if duration is not None and duration < 0:
                logger.warn('Negative duration on event (%s) (%s)',
                            duration, event)
            batch_events.append(new_event)
            if IAnalyticsProgressEvent.providedBy(new_event):
                resource_to_root_context.add((new_event.ResourceId,
                                              new_event.RootContextID))
        except (ValidationError, ValueError) as e:
            # The app may resend events if we err; so we should just log.
            # String values in int fields throw ValueErrors instead of validation
            # errors.
            logger.warning('Malformed events received (event=%s) (%s)', event, e)
            malformed_count += 1

    handled = []
    event_count, invalid_exc_list = handle_events(batch_events, True, handled)

    # if there are valid events notify last seen
    if handled:
        notify_lastseen_event(remote_user, request)
        notify(UserProcessedEventsEvent(remote_user, handled, request))

    # Now broadcast to interested parties that progress may have updated for
    # certain objects within certain contexts. This is probably not useful
    # if our state is not updated in-line above.
    for resource_ntiid, root_context_ntiid in resource_to_root_context:
        resource_obj = find_object_with_ntiid(resource_ntiid)
        root_context = find_object_with_ntiid(root_context_ntiid)
        # This does not do anything for books (since they do not have completion).
        context_provider = ICompletionContextProvider(root_context, None)
        completion_context = context_provider() if context_provider else None
        if      resource_obj is not None \
            and completion_context is not None:
            notify(UserProgressUpdatedEvent(resource_obj,
                                            remote_user,
                                            completion_context))
        else:
            logger.info("Could not find course from (%s) (%s)",
                        resource_ntiid, root_context_ntiid)

    for invalid_exc in invalid_exc_list:
        logger.warning('Invalid events received (%s)', invalid_exc)
        invalid_count += 1
    return event_count, malformed_count, invalid_count


class AnalyticsUpdateMixin(object):
    """
    An analytics view mixin that decides when to process analytics updates.
    """

    def _do_store_analytics(self):
        raise NotImplementedError()

    def store_analytics(self, request):
        if should_create_analytics(request):
            return self._do_store_analytics()
        return hexc.HTTPForbidden(_('Cannot update analytics for this user.'))


@view_config(route_name='objects.generic.traversal',
             context=IEventsCollection,
             renderer='rest',
             request_method='POST',
             permission=nauth.ACT_CREATE)
class BatchEvents(AbstractAuthenticatedView,
                  ModeledContentUploadRequestUtilsMixin,
                  AnalyticsUpdateMixin):
    """
    A view that accepts a batch of analytics events.  The view
    will parse the input and process the events.
    """

    content_predicate = IBatchResourceEvents.providedBy

    def _do_store_analytics(self):
        external_input = self.readInput()
        events = external_input['events']
        total_count = len(events)

        event_count, malformed_count, invalid_count = \
                    _process_batch_events(events, self.remoteUser, self.request)
        if event_count > 10 or malformed_count or invalid_count:
            logger.info("""Received batched analytic events (count=%s) (total_count=%s) (malformed=%s) (invalid=%s)""",
                        event_count, total_count, malformed_count, invalid_count)

        statsd = statsd_client()
        if statsd is not None:
            statsd.incr('nti.analytics.events.received.malformed', malformed_count)
            statsd.incr('nti.analytics.events.received.total', total_count)

        result = LocatedExternalDict()
        result['EventCount'] = event_count
        result['InvalidCount'] = invalid_count
        result['MalformedEventCount'] = malformed_count
        return result

    def _do_call(self):
        return self.store_analytics(self.request)


@view_config(route_name='objects.generic.traversal',
             name=SYNC_PARAMS,
             context=IAnalyticsWorkspace,
             renderer='rest',
             request_method='GET')
class BatchEventParams(AbstractAuthenticatedView):

    def __call__(self):
        # Return our default analytic client params
        client_params = AnalyticsClientParams()
        return client_params


@event.listens_for(Sessions, "after_insert")
def _record_loaded_instances_on_load(unused_mapper, unused_connection, new_session):
    # Capture our new session_id so we can attach to the request
    # After a commit, sqlalchemy purges these potentially-stale
    # attributes.
    new_session._v_session_id = new_session.session_id


@view_config(route_name='objects.generic.traversal',
             name=ANALYTICS_SESSION,
             context=ISessionsCollection,
             renderer='rest',
             request_method='POST',
             permission=nauth.ACT_CREATE)
class AnalyticsSession(AbstractAuthenticatedView,
                       AnalyticsUpdateMixin):

    def _set_cookie(self, request, new_session):
        # If we have current session, fire an event to kill it.
        # Is this what we want?  What about multiple tabs?
        # Will we inadvertantly kill open sessions?
        old_id = get_session_id_from_request(request)
        if old_id is not None:
            user = self.remoteUser
            if user is not None:
                # pylint: disable=no-member
                handle_end_session(user.username, old_id)

        def do_set_cookie():
            request.response.set_cookie(ANALYTICS_SESSION_COOKIE_NAME,
                                        value=str(new_session._v_session_id),
                                        overwrite=True)
        # After committing, set the session_id into the response cookie
        transactions.do_near_end(call=do_set_cookie)

    def _do_store_analytics(self):
        """
        Create a new analytics session and place it in a cookie.
        """
        request = self.request
        user = request.remote_user
        if user is not None:
            # handle session and set cookie
            new_session = handle_new_session(user, request)
            self._set_cookie(request, new_session)
            # notify last seen
            notify_lastseen_event(self.remoteUser, request)
        return request.response

    def __call__(self):
        return self.store_analytics(self.request)


@view_config(route_name='objects.generic.traversal',
             name=END_ANALYTICS_SESSION,
             context=ISessionsCollection,
             renderer='rest',
             request_method='POST',
             permission=nauth.ACT_CREATE)
class EndAnalyticsSession(AbstractAuthenticatedView,
                          ModeledContentUploadRequestUtilsMixin,
                          AnalyticsUpdateMixin):
    """
    Ends an analytic session, defined by information in the
    header or cookie of this request.  Optionally accepts a
    `timestamp` param, allowing the client to specify the
    session end time.

    timestamp
            The (optional) seconds since the epoch marking when
            the session ended.

    batch_events
            The (optional) closed batch_events, occurring at the end of
            session.
    """

    def _do_store_analytics(self):
        """
        End the current analytics session.
        """
        request = self.request
        user = request.remote_user

        values = CaseInsensitiveDict(self.readInput())
        timestamp = values.get('timestamp')
        batch_events = values.get('batch_events')

        if batch_events:
            events = batch_events.get('events')
            if events:
                total_count = len(events)
                event_count, malformed_count, invalid_count = \
                                _process_batch_events(events, self.remoteUser, request)
                logger.info("""Process batched analytic events on session close
                            (count=%s) (total_count=%s) (malformed=%s)
                            (invalid_count=%s)""",
                            event_count, total_count, malformed_count,
                            invalid_count)

        session_id = get_session_id_from_request(request)
        handle_end_session(user, session_id, timestamp=timestamp)
        request.response.delete_cookie(ANALYTICS_SESSION_COOKIE_NAME)

        # notify user last seen
        notify_lastseen_event(self.remoteUser, request)
        # request.response.status_code = 204
        return self.request.response

    def __call__(self):
        return self.store_analytics(self.request)


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             context=ICourseInstance,
             request_method='GET',
             permission=nauth.ACT_READ,
             name="VideoProgress")
class UserCourseVideoProgress(AbstractAuthenticatedView,
                              ModeledContentUploadRequestUtilsMixin):
    """
    For the given course instance, return the progress we have for the user
    on each video in the course.

    On return, the 'LastModified' header will be set, allowing
    the client to specify the 'If-Modified-Since' header for future requests.
    A HTTP-304 will be returned if the results have not changed.
    """

    def __call__(self):
        user = self.getRemoteUser()
        course = self.context

        result = LocatedExternalDict()
        result[StandardExternalFields.CLASS] = 'CourseVideoProgress'
        result[StandardExternalFields.ITEMS] = item_dict = {}
        node_last_modified = None

        video_progress_col = get_video_progress_for_course(user, course)

        for video_progress in video_progress_col:
            rid = video_progress.NTIID
            item_dict[rid] = to_external_object(video_progress)
            node_last_modified = _get_last_mod(video_progress,
                                               node_last_modified)

        # Setting this will enable the renderer to return a 304, if needed.
        self.request.response.last_modified = node_last_modified
        return result


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             context=IUser,
             request_method='POST',
             permission=nauth.ACT_UPDATE,
             name=SET_RESEARCH_VIEW)
class UserResearchStudyView(AbstractAuthenticatedView,
                            ModeledContentUploadRequestUtilsMixin):
    """
    Updates a user's research status.
    """

    def __call__(self):
        values = CaseInsensitiveDict(self.readInput())
        allow_research = values.get('allow_research')
        allow_research = is_true(allow_research)
        user = self.request.context
        set_research_status(user, allow_research)
        logger.info('Setting research status for user (user=%s) (allow_research=%s)',
                    user.username, allow_research)
        return hexc.HTTPNoContent()


class AbstractUserLocationView(AbstractAuthenticatedView):
    """
    Provides a representation of the geographical
    locations of users within a course.
    """

    @Lazy
    def course_start_date(self):
        try:
            # legacy code path, but faster
            entry = self.course.legacy_catalog_entry
        except AttributeError:
            entry = ICourseCatalogEntry(self.course)
        return entry.StartDate

    @Lazy
    def course(self):
        return ICourseInstance(self.context)

    def generate_semester(self):
        # pylint: disable=no-member, using-constant-test
        start_date = self.course_start_date
        start_month = start_date.month if start_date else None
        if start_month < 5:
            semester = _(u'Spring')
        elif start_month < 8:
            semester = _(u'Summer')
        else:
            semester = _(u'Fall')

        start_year = start_date.year if start_date else None
        return '%s %s' % (semester, start_year) if start_date else ''

    def get_data(self, course):
        enrollment_scope = self.request.params.get('enrollment_scope')
        data = get_location_list(course, enrollment_scope)
        return data


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             permission=nauth.ACT_NTI_ADMIN,
             context=ICourseInstance,
             request_method='GET',
             accept='application/json',
             name=GEO_LOCATION_VIEW)
class UserLocationJsonView(AbstractUserLocationView):
    """
    Provides a json representation of the geographical
    locations of users within a course.
    """

    def __call__(self):
        return self.get_data(self.context)


def _tx_string(label):
    if label and isinstance(label, six.text_type):
        label = label.encode('utf-8')
    return label


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             permission=nauth.ACT_NTI_ADMIN,
             context=ICourseInstance,
             request_method='GET',
             accept='text/csv',
             name=GEO_LOCATION_VIEW)
class UserLocationCsvView(AbstractUserLocationView):
    """
    Provides a CSV representation of the geographical
    locations of users within a course.
    """

    def __call__(self):

        def convert_to_utf8(data):
            for key, value in list(data.items()):
                data[key] = _tx_string(value)
            return data

        location_data = self.get_data(self.context)
        if len(location_data) == 0:
            return hexc.HTTPUnprocessableEntity(_(u"No locations were found."))

        stream = BytesIO()
        fieldnames = ['number_of_students', 'city', 'state',
                      'country', 'latitude', 'longitude']
        csv_writer = csv.DictWriter(stream, fieldnames=fieldnames,
                                    extrasaction='ignore')
        csv_writer.writeheader()

        for line in location_data:
            csv_writer.writerow(convert_to_utf8(line))

        response = self.request.response
        response.body = stream.getvalue()
        response.content_type = 'text/csv; charset=UTF-8'
        response.content_disposition = 'attachment; filename="locations.csv"'
        return response


@view_config(route_name='objects.generic.traversal',
             renderer='templates/user_location_map.pt',
             permission=nauth.ACT_NTI_ADMIN,
             context=ICourseInstance,
             request_method='GET',
             accept='text/html',
             name=GEO_LOCATION_VIEW)
class UserLocationHtmlView(AbstractUserLocationView):
    """
    Provides HTML code for a page displaying the geographical
    locations of users within a course, plotted on a map.
    """

    def __call__(self):
        location_data = self.get_data(self.context)
        if not location_data:
            return hexc.HTTPUnprocessableEntity(_(u"No locations were found"))

        locations = []
        options = LocatedExternalDict()
        locations.append([str('Lat'), str('Long'), str('Label')])
        for location in location_data:
            locations.append([location['latitude'],
                              location['longitude'],
                              _tx_string(location['label'])])

        options['locations'] = locations
        # Pass the data separate (and as-is) since our template engine handles
        # encoded items.
        options['location_data'] = location_data
        friendly_name = '%s %s' % (
            self.context.__name__, self.generate_semester()
        )
        options['course_info'] = {
            'course_friendly_name': friendly_name,
            'course_section': self.context.__name__
        }

        return options


class StatsSourceMixin(object):
    """
    Something that looks up a stats source based context
    """

    def _query_source(self, source_iface):
        """
        If we have a context we must use the context specific adapter,
        not the global utility (which queries all users)
        """
        context = find_interface(self.context, IAnalyticsContext, strict=False)
        if context:
            return component.queryAdapter(context, source_iface)
        return component.queryUtility(source_iface)


class WindowedViewMixin(object):

    def _time_param(self, pname):
        timestamp = self.request.params.get(pname)
        timestamp = float(timestamp) if timestamp is not None else None
        return datetime.datetime.utcfromtimestamp(timestamp) if timestamp else None

    @property
    def not_before(self):
        return self._time_param('notBefore')

    @property
    def not_after(self):
        return self._time_param('notAfter')

    def time_window(self):
        not_after = self.not_after
        not_before = self.not_before
        if not_after is None and not_before is None:
            not_after = datetime.datetime.utcnow()
            not_before = not_after - datetime.timedelta(days=self.DEFAULT_WINDOW_DAYS)
        return not_before, not_after


class AbstractHistoricalAnalyticsView(AbstractUserLocationView,
                                      WindowedViewMixin):
    """
    Provides a collection of recent sessions the users has had.
    By default this view returns 30 days worth of sessions. Query
    params of start and end can be provided to give a date range
    """

    DEFAULT_WINDOW_DAYS = 30
    DEFAULT_LIMIT = 10

    @property
    def _limit(self):
        return int(self.request.params.get('limit', self.DEFAULT_LIMIT))

    def _make_external(self, o):
        return o

    def _analytics_context(self):
        return find_interface(self.context, IAnalyticsContext, strict=False)

    def _get_raw(self, unused_not_before, unused_not_after):
        return []

    def __call__(self):
        not_before = self.not_before
        not_after = self.not_after

        raw = self._get_raw(not_before, not_after)
        objects = [self._make_external(obj) for obj in raw]
        options = LocatedExternalDict()
        options.__parent__ = self.request.context
        options.__name__ = self.request.view_name
        options[ITEMS] = objects
        options[ITEM_COUNT] = options[TOTAL] = len(objects)
        return options


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             context=ISessionsCollection,
             request_method='GET',
             permission=nauth.ACT_READ)
class UserRecentSessions(AbstractHistoricalAnalyticsView):
    """
    Provides a collection of recent sessions the users has had.
    By default this view returns 30 days worth of sessions. Query
    params of start and end can be provided to give a date range
    """

    def _make_external(self, session): # pylint: disable=arguments-differ
        return IAnalyticsSession(session)

    def _analytics_context(self):
        return find_interface(self.context, IUser, strict=False)

    def _get_raw(self, not_before, not_after):
        context = self._analytics_context()
        # By default, exclude NT admins
        nt_admins = get_admins()
        if not_before and not_after:
            sessions = get_user_sessions(context,
                                         timestamp=not_before,
                                         max_timestamp=not_after,
                                         excluded_users=nt_admins)
        else:
            limit = self._limit
            not_after = not_after or datetime.datetime.utcnow()
            sessions = get_recent_user_sessions(context,
                                                limit=limit,
                                                not_after=not_after,
                                                excluded_users=nt_admins)
        return sessions


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             context=IEventsCollection,
             request_method='GET',
             permission=nauth.ACT_READ)
class GetActivity(AbstractHistoricalAnalyticsView):

    def _make_external(self, event): # pylint: disable=arguments-differ
        return to_external_object(event, name='summary')

    def _get_raw(self, not_before, not_after):
        context = self._analytics_context()
        if context:
            source = IActivitySource(self._analytics_context())
        else:
            source = component.getUtility(IActivitySource)

        #They gave us a time window
        if not_before and not_after:
            kwargs = {'timestamp': not_before,
                      'max_timestamp': not_after}
        else:
            limit = self._limit
            not_after = not_after or datetime.datetime.utcnow()

            kwargs = {'limit': limit,
                      'max_timestamp': not_after}

        kwargs['order_by'] = 'timestamp'
        return source.activity(**kwargs)


@view_config(route_name='objects.generic.traversal',
             name=ACTIVE_SESSION_COUNT,
             context=ISessionsCollection,
             renderer='rest',
             request_method='GET',
             permission=nauth.ACT_READ)
class AnalyticsSessionCount(AbstractAuthenticatedView):

    def __call__(self):
        if not is_admin_or_site_admin(self.remoteUser):
            raise hexc.HTTPForbidden()
        stats_provider = component.queryUtility(IActiveSessionStatsSource)
        if not stats_provider:
            raise hexc.HTTPNotFound()
        return stats_provider()


@view_config(route_name='objects.generic.traversal',
             name=ACTIVE_TIMES_SUMMARY,
             context=IAnalyticsWorkspace,
             renderer='rest',
             request_method='GET',
             permission=nauth.ACT_READ)
class AnalyticsTimeSummary(AbstractAuthenticatedView, StatsSourceMixin):
    """
    Builds heat map information for a matrix of weekday and hours.
    """

    def times_to_consider(self, as_of_time=None, weeks=4):
        """
        We want `weeks` full weeks of data.
        """
        if not as_of_time:
            as_of_time = datetime.datetime.utcnow()

        # Go back to the beginning of today. It
        # will be the *exclusive* end of the range
        end_date = datetime.datetime(as_of_time.year,
                                     as_of_time.month,
                                     as_of_time.day)

        # The start of our range will be `weeks` weeks before
        start_date = end_date - datetime.timedelta(weeks=weeks)
        return start_date, end_date

    def __call__(self):
        weeks = self.request.params.get('weeks', 4)
        try:
            weeks = int(weeks)
        except ValueError:
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                 'message': _(u"Weeks must be an integer."),
                             },
                             None)

        source = self._query_source(IActiveTimesStatsSource)
        if source is None:
            raise hexc.HTTPNotFound()

        start, end = self.times_to_consider(weeks=weeks)

        stats = source.active_times_for_window(start, end)

        result = LocatedExternalDict()
        result.__parent__ = self.request.context
        result.__name__ = self.request.view_name

        result['StartTime'] = start
        result['EndTime'] = end

        items = {}
        for idx, day in enumerate(calendar.day_name):
            items[day] = [stats[idx][hour].Count for hour in range(0, 24)]

        result['WeekDays'] = items

        # These results will not update more frequently than daily
        interface.alsoProvides(result, _IDailyResults)

        return result


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             name=ACTIVITY_SUMMARY_BY_DATE,
             context=IAnalyticsWorkspace,
             request_method='GET',
             permission=nauth.ACT_READ)
class ActivitySummaryByDate(AbstractUserLocationView, StatsSourceMixin, WindowedViewMixin):
    """
    Provides a collection of recent sessions the users has had.
    By default this view returns 30 days worth of sessions. Query
    params of start and end can be provided to give a date range
    """

    DEFAULT_WINDOW_DAYS = 90

    def __call__(self):
        not_before, not_after = self.time_window()
        source = self._query_source(IDailyActivityStatsSource)
        if source is None:
            raise hexc.HTTPNotFound()
        stats = source.stats_for_window(not_before, not_after)

        result = LocatedExternalDict()
        result.__parent__ = self.request.context
        result.__name__ = self.request.view_name

        result['StartTime'] = not_before
        result['EndTime'] = not_after
        result['Dates'] = {
            k.strftime('%Y-%m-%d'): v.Count for k, v in stats.items()
        }

        cache_hint = _IDailyResults
        if not_after and not_before:
            cache_hint = _IHistoricalResults
        interface.alsoProvides(result, cache_hint)

        return result


@interface.implementer(IResponseCacheController)
class AbstractStatsCacheControl(object):

    def __init__(self, context, request=None):
        self.context = context
        self.request = request

    def __call__(self, context, system):
        return default_cache_controller(context, system)


class DailyCacheControl(AbstractStatsCacheControl):

    def __call__(self, context, system):
        resp = super(DailyCacheControl, self).__call__(context, system)
        resp.cache_control.must_revalidate = False
        now = datetime.datetime.utcnow()

        # Go back to the beginning of today. It
        # will be the *exclusive* end of the range
        expires = datetime.datetime(now.year,
                                    now.month,
                                    now.day)
        expires = (expires + datetime.timedelta(days=1))
        resp.cache_control.max_age = int((expires - now).total_seconds())
        return resp


class HistoricalCacheControl(AbstractStatsCacheControl):

    def __call__(self, context, system):
        resp = super(HistoricalCacheControl, self).__call__(context, system)
        resp.cache_control.max_age = 24*60*60
        resp.cache_control.must_revalidate = False
        return resp


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             name=ACTIVE_USERS,
             context=IAnalyticsWorkspace,
             request_method='GET',
             permission=nauth.ACT_READ)
class ActiveUsers(AbstractUserLocationView,
                  StatsSourceMixin,
                  WindowedViewMixin,
                  BatchingUtilsMixin):

    _DEFAULT_BATCH_SIZE = 10
    _DEFAULT_BATCH_START = 0

    DEFAULT_WINDOW_DAYS = 30

    def __call__(self):
        users_source = self._query_source(IActiveUsersSource)
        if not users_source:
            raise hexc.HTTPNotFound()

        not_before, not_after = self.time_window()

        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        users = users_source.users(timestamp=not_before,
                                   max_timestamp=not_after)
        # Only include current site users.
        # XXX: This is nice for this view, but it does not address similar
        # issues in the views above (activity stats views).
        current_sitename = getSite().__name__
        users = (x for x in users if get_user_creation_sitename(x) == current_sitename)
        self._batch_items_iterable(result, users)

        cache_hint = _IDailyResults
        if self.not_after:
            cache_hint = _IHistoricalResults
        interface.alsoProvides(result, cache_hint)
        return result

@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               context=INTIVideo,
               request_method='GET',
               permission=nauth.ACT_READ)
class VideoResumeInfo(AbstractAuthenticatedView):

    @Lazy
    def course(self):
        return ICourseInstance(self.request)

    @Lazy
    def user(self):
        username_param = self.request.params.get('username', None)
        if username_param:
            return User.get_user(username_param)
        return self.remoteUser

    def make_result(self):
        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        result['NTIID'] = self.context.ntiid
        result['Course'] = self.course.ntiid
        result['Username'] = self.user.username
        return result

    def _do_check_permission(self, perm=ACT_VIEW_DETAILED_CONTENT_USAGE):
        """
        Our view callable predicate ensures read access to the
        resource which is a good first pass, but we need something
        slightly more complicated. Our full permission scheme is that
        users can always get this information for themselves, but to
        get this information for someone else you need an appropriate
        permission on the user, course, or user and course (which we can represent by
        the enrollment record)
        """
        if not self.user:
            raise hexc.HTTPNotFound()

        # We check the user first, and then the enrollment record. The
        # idea being that someone that can manage some subset of users
        # might have this permission managed at a user level ("manager
        # role", "site admin", etc.), while instructors would get it
        # at the enrollment record level.
        if has_permission(perm, self.user):
            return True

        if has_permission(perm, self.course):
            return True
        
        enrollment = component.queryMultiAdapter((self.course, self.user),
                                                 ICourseInstanceEnrollment)
        if enrollment and has_permission(perm, enrollment):
            return True

        raise hexc.HTTPForbidden()


    @view_config(name='resume_info')
    def get_resume_info(self):
        self._do_check_permission()
        return self._do_get_resume_info()

    def _do_get_resume_info(self):
        
        # As a generalization we could adapt the video and enrollment
        # record (user x course) to some sort of IVideoResumeInformation
        # and encapsulate this logic there. Do we need to do this sort
        # of thing in other contexts?
        events = get_video_views_for_ntiid(self.context.ntiid,
                                           user=self.user,
                                           course=self.course,
                                           order_by='timestamp',
                                           limit=1)

        event = events[0] if events else None
        
        result = self.make_result()

        if event:
            result['MaxDuration'] = event.MaxDuration
            # When a user starts watching a video we get an initial watch
            # event with a video_start_time, but no duration, and no video_end_time.
            # if they close the window or we don't get any updates for that event
            # the resume_info is that starting point.
            #
            # Then we start getting heartbeats. We still don't have an end_time
            # but we do get a Duration (time_length) that is the offset from the start
            # time to the playhead.
            playhead = event.video_end_time
            if not playhead:
                playhead = event.video_start_time + (event.time_length or 0)
            result['ResumeSeconds'] = playhead

        return result

    @view_config(name="watched_segments")
    def get_watched_segments(self):
        self._do_check_permission()
        
        segments = get_watched_segments_for_ntiid(self.context.ntiid,
                                                  user=self.user,
                                                  course=self.course)

        result = self._do_get_resume_info()
        
        def _make_segment(start, end, count):
            return {
                'video_start_time': start,
                'video_end_time': end,
                'Count': count
            }
        result['WatchedSegments'] = [_make_segment(*s) for s in segments]

        return result

        
