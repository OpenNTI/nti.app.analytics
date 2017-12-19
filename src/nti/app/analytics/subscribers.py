# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component

from nti.analytics.sessions import handle_end_session

from nti.appserver.interfaces import IUserLogoutEvent
from nti.app.analytics.utils import get_session_id_from_request

logger = __import__('logging').getLogger(__name__)


@component.adapter( IUserLogoutEvent )
def _user_logout_event(event):
	"""
	When a user logs out, terminate any sessions tied to the request.
	We only do this for cookies (e.g. webapp/mobile) since other clients
	manage their own sessions explicitly.
	"""
	session_id = get_session_id_from_request(event.request)

	if session_id is not None:
		user = event.user
		username = getattr(user, 'username', None)
		if username is None:
			username = getattr(event.request,'remote_user', None)
		handle_end_session(username, session_id)
