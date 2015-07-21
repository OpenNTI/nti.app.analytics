#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Adapters for application-level events.

.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from datetime import datetime

from six import integer_types

from zope import interface
from zope import component

from nti.app.products.courseware.interfaces import IViewStats
from nti.dataserver.contenttypes.forums.interfaces import ITopic

from nti.assessment.interfaces import IQAssignment
from nti.assessment.interfaces import IQuestionSet

from nti.dataserver.interfaces import INote
from nti.dataserver.interfaces import IUser

from nti.analytics import has_analytics
from nti.analytics.interfaces import IProgress

from nti.analytics.assessments import get_assignment_for_user
from nti.analytics.assessments import get_self_assessments_for_user_and_id

from nti.analytics.boards import get_topic_views

from nti.analytics.resource_tags import get_note_views

from nti.analytics.progress import DefaultProgress

@interface.implementer( IProgress )
@component.adapter( IUser, IQAssignment )
def _assignment_progress_for_user( user, assignment ):
	"""
	Given an assignment and a user, we
	attempt to determine the amount of progress the user
	has made on the assignment.  If we have nothing in which to
	gauge progress, we return None.
	"""
	# In local tests, about 100 objects are decorated in about 1s;
	# this is in UCOL with a lot of assignments but few assessments.
	assignment_id = getattr( assignment, 'ntiid', None )
	assignment_records = get_assignment_for_user( user, assignment_id )
	result = None
	if assignment_records:
		# Simplistic implementation
		last_mod = max( (x.timestamp for x in assignment_records) )
		result = DefaultProgress( assignment_id, 1, 1, True, last_modified=last_mod )
	return result

@interface.implementer( IProgress )
@component.adapter( IUser, IQuestionSet )
def _assessment_progress_for_user( user, assessment ):
	"""
	Given a generic assessment and a user, we
	attempt to determine the amount of progress the user
	has made on the assignment.  If we have nothing in which to
	gauge progress, we return None.
	"""
	# To properly check for assignment, we need the course to see
	# what the assignment ntiids are versus the possible self-assessment ids.
	# Maybe we're better off checking for assignment or self-assessment.
	# If we have a cache, the cost is trivial.
	# Or we only care about possible self-assessments here; if we have a record
	# great, else we do not return anything.
	assessment_id = getattr( assessment, 'ntiid', None )
	assessment_records = get_self_assessments_for_user_and_id( user, assessment_id )
	result = None
	if assessment_records:
		# Simplistic implementation
		last_mod = max( (x.timestamp for x in assessment_records) )
		result = DefaultProgress( assessment_id, 1, 1, True, last_modified=last_mod )
	return result

class _ViewStats( object ):

	def __init__(self, view_count, new_reply_count_for_user=0 ):
		self.view_count = view_count
		self.new_reply_count_for_user = new_reply_count_for_user

def _get_stats( records, replies=None, user=None ):
	count = len( records ) if records else 0
	reply_count = len( replies ) if replies else 0
	if user is None:
		# We just want the count
		result = _ViewStats( count, 0 )
	elif not records:
		# User but no views
		result = _ViewStats( count, reply_count )
	else:
		# Ok, find out how many replies since our user viewed the object.
		user_view_times = [x.timestamp for x in records if x.user == user and x.timestamp]

		if user_view_times:
			user_last_viewed = max( user_view_times ) if user_view_times else 0
			new_reply_count_for_user = 0
			for reply in replies:
				reply_created_time = reply.createdTime
				if isinstance( reply_created_time, (integer_types, float) ):
					reply_created_time = datetime.utcfromtimestamp( reply_created_time )
				if reply_created_time and reply_created_time > user_last_viewed:
					new_reply_count_for_user += 1
			result = _ViewStats( count, new_reply_count_for_user )
		else:
			result = _ViewStats( count, reply_count )
	return result

@interface.implementer( IViewStats )
@component.adapter( ITopic )
def _topic_view_stats( topic ):
	result = None
	if has_analytics():
		records = get_topic_views( topic=topic )
		result = _get_stats( records )
	return result

@interface.implementer( IViewStats )
@component.adapter( ITopic, IUser )
def _topic_view_stats_for_user( topic, user ):
	result = None
	if has_analytics():
		records = get_topic_views( topic=topic )
		replies = topic.values()
		result = _get_stats( records, replies, user )
	return result

@interface.implementer( IViewStats )
@component.adapter( INote )
def _note_view_stats( note ):
	result = None
	if has_analytics():
		records = get_note_views( note=note )
		result = _get_stats( records )
	return result

@interface.implementer( IViewStats )
@component.adapter( INote, IUser )
def _note_view_stats_for_user( note, user ):
	result = None
	if has_analytics():
		records = get_note_views( note=note )
		replies = note.referents
		result = _get_stats( records, replies, user )
	return result

