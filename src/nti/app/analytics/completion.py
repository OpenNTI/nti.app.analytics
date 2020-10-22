#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import urlparse

from zope import interface
from zope import component

from nti.analytics.progress import get_progress_for_video_views
from nti.analytics.progress import get_progress_for_resource_views
from nti.analytics.progress import get_progress_for_resource_container

from nti.analytics.resource_views import get_video_views_for_ntiid
from nti.analytics.resource_views import get_resource_views_for_ntiid

from nti.contentlibrary.interfaces import IContentUnit

from nti.contenttypes.completion.interfaces import IProgress

from nti.contenttypes.courses.interfaces import ICourseInstance

from nti.contenttypes.presentation.interfaces import INTIVideo
from nti.contenttypes.presentation.interfaces import INTIRelatedWorkRef

from nti.coremetadata.interfaces import IUser

from nti.ntiids.ntiids import find_object_with_ntiid

logger = __import__('logging').getLogger(__name__)


def _has_href_fragment(node, children):
    def _has_frag(node):
        return urlparse.urldefrag(node.href)[1]

    # A fragment if we have a frag or if any of our children do
    return bool(_has_frag(node)
                or _has_frag(next(iter(children))))


def _is_page_container(node):
    # Node is only page container if it has children and
    # does not have a fragment in its href.
    children = getattr(node, 'children', None)
    return bool(children and not _has_href_fragment(node, children))


@component.adapter(IUser, INTIVideo, ICourseInstance)
@interface.implementer(IProgress)
def video_progress(user, video, course):
    resource_views = get_video_views_for_ntiid(video.ntiid, user, course)
    result = get_progress_for_video_views(video.ntiid,
                                          resource_views,
                                          video,
                                          user,
                                          course)
    return result


@component.adapter(IUser, IContentUnit, ICourseInstance)
@interface.implementer(IProgress)
def content_progress(user, content_unit, course):
    """
    Return the :class:`IProgress` associated with the :class:`IContentUnit`.
    """
    content_ntiid = content_unit.ntiid
    if _is_page_container(content_unit):
        # Top level container with pages (?)
        child_views_dict = {}
        # TODO: Some clients might be sending in view events for the container
        # itself instead of the first page.  We add that in, even through it
        # might disturb the accuracy of our results.
        parent_views = get_resource_views_for_ntiid(content_ntiid,
                                                    user,
                                                    course)
        child_views_dict[content_ntiid] = parent_views

        for child in content_unit.children:
            child_views = get_resource_views_for_ntiid(child.ntiid,
                                                       user,
                                                       course)
            child_views_dict[child.ntiid] = child_views
        result = get_progress_for_resource_container(content_ntiid,
                                                     child_views_dict,
                                                     content_unit,
                                                     user,
                                                     course)
    else:
        resource_views = get_resource_views_for_ntiid(content_ntiid,
                                                      user,
                                                      course)
        result = get_progress_for_resource_views(content_ntiid,
                                                 resource_views,
                                                 content_unit,
                                                 user,
                                                 course)
    return result


@component.adapter(IUser, INTIRelatedWorkRef, ICourseInstance)
@interface.implementer(IProgress)
def related_work_ref_progress(user, ref, course):
    result = None
    is_reading = False
    target_ntiid = getattr(ref, 'target', '')
    if target_ntiid:
        target = find_object_with_ntiid(target_ntiid)
        if IContentUnit.providedBy(target):
            is_reading = True
            # We want to handle readings particularly
            result = content_progress(user, target, course)
    if not is_reading:
        resource_views = get_resource_views_for_ntiid(ref.ntiid, user, course)
        result = get_progress_for_resource_views(ref.ntiid,
                                                 resource_views,
                                                 ref,
                                                 user,
                                                 course)
    return result

