#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time

from datetime import datetime
from datetime import timedelta

from ZODB.POSException import POSError

from zope import component
from zope import interface
from zope.container.contained import Contained
from zope.traversing.interfaces import IPathAdapter

from pyramid.view import view_config

from nti.analytics import get_factory
from nti.analytics import QUEUE_NAMES

from nti.analytics.interfaces import IUserResearchStatus

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.dataserver.authorization import ACT_MODERATE
from nti.dataserver.authorization import ACT_READ
from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout
from nti.dataserver.interfaces import IUser

from nti.externalization.interfaces import LocatedExternalDict

from . import ANALYTICS

@interface.implementer(IPathAdapter)
class AnalyticsPathAdapter(Contained):

	__name__ = ANALYTICS

	def __init__(self, context, request):
		self.context = context
		self.request = request
		self.__parent__ = context

_view_defaults = dict(route_name='objects.generic.traversal',
					  renderer='rest',
					  permission=ACT_READ,
					  context=AnalyticsPathAdapter,
					  request_method='GET')
_post_view_defaults = _view_defaults.copy()
_post_view_defaults['request_method'] = 'POST'

def _make_min_max_btree_range(search_term):
	min_inclusive = search_term # start here
	max_exclusive = search_term[0:-1] + unichr(ord(search_term[-1]) + 1)
	return min_inclusive, max_exclusive

def username_search(search_term):
	min_inclusive, max_exclusive = _make_min_max_btree_range(search_term)
	dataserver = component.getUtility(IDataserver)
	_users = IShardLayout(dataserver).users_folder
	usernames = list(_users.iterkeys(min_inclusive, max_exclusive, excludemax=True))
	return usernames

@view_config(route_name='objects.generic.traversal',
			 name='queue_info',
			 renderer='rest',
			 request_method='GET',
			 permission=ACT_MODERATE)
def queue_info(request):
	result = LocatedExternalDict()
	factory = get_factory()

	for name in QUEUE_NAMES:
		queue_info = LocatedExternalDict()
		result[ name ] = queue_info
		queue = factory.get_queue( name )
		queue_info[ 'queue_length' ] = len(queue)
		queue_info[ 'failed_length' ] = len(queue.get_failed_queue())

	return result

@view_config(route_name='objects.generic.traversal',
			 name='empty_queue',
			 renderer='rest',
			 request_method='POST',
			 permission=ACT_MODERATE)
def empty_queue(request):
	result = LocatedExternalDict()
	factory = get_factory()
	queue_names = QUEUE_NAMES
	now = time.time()

	for name in queue_names:
		queue = factory.get_queue( name )
		queue_count = len( queue )
		queue.empty()

		fail_queue = queue.get_failed_queue()
		failed_count = len( fail_queue )
		fail_queue.empty()

		if queue_count or failed_count:
			logger.info( 	'Emptied analytics processing queue (%s) (count=%s) (fail_count=%s)',
							name, queue_count, failed_count )

		queue_stat = LocatedExternalDict()
		queue_stat['Total'] = queue_count
		queue_stat['Failed total'] = failed_count
		result[ name ] = queue_stat

	elapsed = time.time() - now
	logger.info( 'Emptied analytics processing queue (time=%s)', elapsed )
	return result


@view_config(route_name='objects.generic.traversal',
			 name='user_research_stats',
			 renderer='rest',
			 request_method='GET',
			 permission=ACT_MODERATE,
			 context=AnalyticsPathAdapter)
class UserResearchStatsView(AbstractAuthenticatedView):

	def __call__( self ):
		result = LocatedExternalDict()

		dataserver = component.getUtility(IDataserver)
		users_folder = IShardLayout(dataserver).users_folder

		allow_count = deny_count = neither_count = 0

		now = datetime.utcnow()
		year_ago = now - timedelta( days=365 )

		# This is pretty slow.
		for user in users_folder.values():
			if not IUser.providedBy(user):
				continue

			try:
				research_status = IUserResearchStatus( user )
			except POSError:
				continue

			last_mod = research_status.lastModified
			if last_mod is not None:
				# First, find the year+ older entries; they are promptable.
				if datetime.utcfromtimestamp( last_mod ) < year_ago:
					neither_count +=1
					continue

			if research_status.allow_research:
				allow_count += 1
			else:
				deny_count += 1

		result['DenyResearchCount'] = deny_count
		result['AllowResearchCount'] = allow_count
		result['ToBePromptedCount'] = neither_count
		return result
