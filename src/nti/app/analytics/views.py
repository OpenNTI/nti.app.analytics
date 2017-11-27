#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id: views.py 122769 2017-10-04 21:56:03Z chris.utz $
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import csv
import six
import calendar
import datetime
from io import BytesIO

from requests.structures import CaseInsensitiveDict

from zope import component

from zope.cachedescriptors.property import Lazy

from zope.schema.interfaces import ValidationError

from pyramid.view import view_config

from pyramid import httpexceptions as hexc

from nti.analytics.interfaces import IAnalyticsContext
from nti.analytics.interfaces import IAnalyticsSession
from nti.analytics.interfaces import IAnalyticsSessions
from nti.analytics.interfaces import IBatchResourceEvents

from nti.analytics.locations import get_location_list

from nti.analytics.model import AnalyticsClientParams

from nti.app.analytics import MessageFactory as _

from nti.app.analytics import SYNC_PARAMS
from nti.app.analytics import ANALYTICS_SESSION
from nti.app.analytics import ACTIVE_SESSION_COUNT
from nti.app.analytics import ACTIVE_TIMES_SUMMARY
from nti.app.analytics import END_ANALYTICS_SESSION
from nti.app.analytics import ACTIVITY_SUMMARY_BY_DATE

from nti.analytics.resource_views import handle_events
from nti.analytics.resource_views import get_progress_for_ntiid
from nti.analytics.resource_views import get_video_progress_for_course

from nti.analytics.sessions import update_session
from nti.analytics.sessions import get_user_sessions
from nti.analytics.sessions import handle_end_session
from nti.analytics.sessions import handle_new_session

from nti.analytics.progress import get_assessment_progresses_for_course

from nti.analytics.stats.interfaces import IActiveTimesStatsSource
from nti.analytics.stats.interfaces import IActiveSessionStatsSource
from nti.analytics.stats.interfaces import IDailyActivityStatsSource

from nti.app.analytics.interfaces import IEventsCollection
from nti.app.analytics.interfaces import IAnalyticsWorkspace
from nti.app.analytics.interfaces import ISessionsCollection

from nti.app.analytics.utils import set_research_status

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.common.string import is_true

from nti.contentlibrary.indexed_data import get_catalog

from nti.contenttypes.courses.interfaces import ICourseInstance
from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode
from nti.contenttypes.courses.interfaces import ICourseCatalogEntry

from nti.contenttypes.presentation import ALL_PRESENTATION_ASSETS_INTERFACES

from nti.contenttypes.presentation.interfaces import IConcreteAsset
from nti.contenttypes.presentation.interfaces import INTIRelatedWorkRef

from nti.dataserver import authorization as nauth

from nti.dataserver.authorization import is_admin_or_site_admin

from nti.dataserver.interfaces import IUser

from nti.externalization import internalization

from nti.externalization.externalization import to_external_object

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.ntiids.ntiids import find_object_with_ntiid

from nti.site.site import get_component_hierarchy_names

from nti.traversal.traversal import find_interface

ITEMS = StandardExternalFields.ITEMS
TOTAL = StandardExternalFields.TOTAL
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

GEO_LOCATION_VIEW = 'GeoLocations'
SET_RESEARCH_VIEW = 'SetUserResearch'

logger = __import__('logging').getLogger(__name__)


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


def _process_batch_events(events):
    """
    Process the events, returning a tuple of events queued and malformed events.
    """
    batch_events = []
    invalid_count = 0
    malformed_count = 0

    # Lets hand-internalize these objects one-by-one so that we
    # can exclude any malformed objects and process the proper events.
    for event in events:
        factory = internalization.find_factory_for(event)
        if factory is None:
            logger.warn('Malformed events received (mime_type=%s) (event=%s)',
                        event.get('MimeType'), event)
            malformed_count += 1
            continue

        new_event = factory()
        try:
            internalization.update_from_external_object(new_event, event)
            batch_events.append(new_event)
        except (ValidationError, ValueError) as e:
            # The app may resend events if we err; so we should just log.
            # String values in int fields throw ValueErrors instead of validation
            # errors.
            logger.warn('Malformed events received (event=%s) (%s)', event, e)
            malformed_count += 1

    event_count, invalid_exc = handle_events(batch_events)
    for invalid_exc in invalid_exc:
        logger.warn('Invalid events received (%s)', invalid_exc)
        invalid_count += 1
    return event_count, malformed_count, invalid_count


@view_config(route_name='objects.generic.traversal',
             context=IEventsCollection,
             renderer='rest',
             request_method='POST',
             permission=nauth.ACT_CREATE)
class BatchEvents(AbstractAuthenticatedView,
                  ModeledContentUploadRequestUtilsMixin):
    """
    A view that accepts a batch of analytics events.  The view
    will parse the input and process the events (e.g. queueing).
    """

    content_predicate = IBatchResourceEvents.providedBy

    def _do_call(self):
        external_input = self.readInput()
        events = external_input['events']
        total_count = len(events)

        event_count, malformed_count, invalid_count = _process_batch_events(events)
        logger.info('Received batched analytic events (count=%s) (total_count=%s) (malformed=%s) (invalid=%s)',
                    event_count, total_count, malformed_count, invalid_count)

        result = LocatedExternalDict()
        result['EventCount'] = event_count
        result['InvalidCount'] = invalid_count
        result['MalformedEventCount'] = malformed_count
        return result


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


@view_config(route_name='objects.generic.traversal',
             name=ANALYTICS_SESSION,
             context=ISessionsCollection,
             renderer='rest',
             request_method='POST',
             permission=nauth.ACT_CREATE)
class AnalyticsSession(AbstractAuthenticatedView):

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
             context=ISessionsCollection,
             renderer='rest',
             request_method='POST',
             permission=nauth.ACT_CREATE)
class EndAnalyticsSession(AbstractAuthenticatedView,
                          ModeledContentUploadRequestUtilsMixin):
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
        timestamp = values.get('timestamp')
        batch_events = values.get('batch_events')

        if batch_events:
            events = batch_events.get('events')
            if events:
                total_count = len(events)
                event_count, malformed_count, invalid_count = _process_batch_events(events)
                logger.info('Process batched analytic events on session close (count=%s) (total_count=%s) (malformed=%s) (invalid_count=%s)',
                            event_count, total_count, malformed_count, invalid_count)

        handle_end_session(user, request, timestamp=timestamp)
        return hexc.HTTPNoContent()


@view_config(route_name='objects.generic.traversal',
             context=ISessionsCollection,
             renderer='rest',
             request_method='POST',
             permission=nauth.ACT_CREATE)
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
        factory = internalization.find_factory_for(external_input)
        sessions = factory()
        internalization.update_from_external_object(sessions, external_input)

        ip_addr = getattr(request, 'remote_addr', None)
        user_agent = getattr(request, 'user_agent', None)

        results = []
        for session in sessions.sessions:
            try:
                result = update_session(session, user,
                                        user_agent=user_agent,
                                        ip_addr=ip_addr)
                results.append(result)
            except ValueError as e:
                # Append invalid session information.
                # We still return a 200 though.
                val = dict()
                val['Error'] = e.message
                results.append(val)
        return results


def _get_ntiids(obj, accum):
    obj = IConcreteAsset(obj, obj)
    attrs_to_check = ('ntiid',)
    if INTIRelatedWorkRef.providedBy(obj):
        attrs_to_check = ('ntiid', 'href')

    for attr in attrs_to_check:
        ntiid_val = getattr(obj, attr, None)
        if ntiid_val is not None:
            accum.add(ntiid_val)

    try:
        for item in obj.items or ():
            _get_ntiids(item, accum)
    except AttributeError:
        pass


def _get_legacy_progress_ntiids(unit, accum):
    if unit is None:
        return
    else:
        _get_ntiids(unit, accum)
        for ntiid in unit.embeddedContainerNTIIDs:
            accum.add(ntiid)
            obj = find_object_with_ntiid(ntiid)
            # If a related work ref, get the target.
            if hasattr(obj, 'target'):
                accum.add(obj.target)
        for child in unit.children:
            _get_legacy_progress_ntiids(child, accum)


def _get_lesson_items(lesson):
    """
    For lessons, iterate and retrieve ntiids.
    """
    result = set()
    for group in lesson or ():
        result.update(group.items or ())
    return result


def _get_lesson_progress_ntiids(lesson, lesson_ntiid):
    results = set()
    catalog = get_catalog()
    rs = catalog.search_objects(container_ntiids=lesson_ntiid,
                                sites=get_component_hierarchy_names(),
                                provided=ALL_PRESENTATION_ASSETS_INTERFACES)
    contained_objects = tuple(rs)
    if not contained_objects and lesson is not None:
        # If we have a lesson, iterate through
        contained_objects = _get_lesson_items(lesson)

    for contained_object in contained_objects or ():
        _get_ntiids(contained_object, results)
    return results


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             context=ICourseOutlineContentNode,
             request_method='GET',
             permission=nauth.ACT_READ,
             name="Progress")
class CourseOutlineNodeProgress(AbstractAuthenticatedView,
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
        ntiid = self.context.LessonOverviewNTIID

        if ntiid:
            lesson = find_object_with_ntiid(ntiid)
            node_ntiids = _get_lesson_progress_ntiids(lesson, ntiid)
        else:
            # Legacy
            node_ntiids = set()
            ntiid = self.context.ContentNTIID
            lesson = find_object_with_ntiid(ntiid)
            _get_legacy_progress_ntiids(lesson, node_ntiids)

        result = LocatedExternalDict()
        result[StandardExternalFields.CLASS] = 'CourseOutlineNodeProgress'
        result[StandardExternalFields.MIMETYPE] = 'application/vnd.nextthought.progresscontainer'
        result[StandardExternalFields.ITEMS] = item_dict = {}

        node_last_modified = None

        # Get progress for resource/videos
        for node_ntiid in node_ntiids or ():
            # Can improve this if we can distinguish between video and other.
            node_progress = get_progress_for_ntiid(user, node_ntiid)
            if node_progress:
                item_dict[node_ntiid] = to_external_object(node_progress)
                node_last_modified = _get_last_mod(node_progress,
                                                   node_last_modified)

        # Get progress for self-assessments and assignments
        try:
            course = find_interface(lesson, ICourseInstance, strict=False)
            if course is None:
                ntiid = self.context.ContentNTIID
                content_unit = find_object_with_ntiid(ntiid)
                course = ICourseInstance(content_unit)
        except TypeError:
            logger.warn('No course found for content unit; cannot return progress for assessments (%s)',
                        ntiid)
            course = None

        if course is not None:
            # Gathering all assignments/self-assessments for course.
            # May be cheaper than finding just for our unit.
            progresses = get_assessment_progresses_for_course(user, course)
            for progress in progresses:
                item_dict[progress.progress_id] = to_external_object(progress)
                node_last_modified = _get_last_mod(progress,
                                                   node_last_modified)

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
             name="VideoProgress")
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

        video_progress_col = get_video_progress_for_course(user, course)

        for video_progress in video_progress_col:
            rid = video_progress.ResourceID
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
        time = self.request.params.get(pname)
        time = float(time) if time is not None else None
        return datetime.datetime.utcfromtimestamp(time) if time else None

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


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             context=ISessionsCollection,
             request_method='GET',
             permission=nauth.ACT_READ)
class UserRecentSessions(AbstractUserLocationView, WindowedViewMixin):
    """
    Provides a collection of recent sessions the users has had.
    By default this view returns 30 days worth of sessions. Query
    params of start and end can be provided to give a date range
    """

    DEFAULT_WINDOW_DAYS = 30

    def _make_session(self, session):
        return IAnalyticsSession(session)

    def __call__(self):
        user_context = find_interface(self.context, IUser, strict=False)

        not_before, not_after = self.time_window()

        sessions = get_user_sessions(user_context,
                                     timestamp=not_before,
                                     max_timestamp=not_after)
        sessions = [self._make_session(s) for s in sessions]

        options = LocatedExternalDict()
        options.__parent__ = self.request.context
        options.__name__ = self.request.view_name
        options[ITEMS] = sessions
        options[ITEM_COUNT] = options[TOTAL] = len(sessions)
        return options


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
            as_of_time = datetime.datetime.now()

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
        return result
