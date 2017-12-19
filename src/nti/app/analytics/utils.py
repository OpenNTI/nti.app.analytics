#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from datetime import datetime

from zope.event import notify

from ZODB.interfaces import IBroken

from nti.analytics.interfaces import IUserResearchStatus

from nti.analytics.model import delete_research_status
from nti.analytics.model import UserResearchStatusEvent

from nti.app.analytics import ANALYTICS_SESSION_HEADER
from nti.app.analytics import ANALYTICS_SESSION_COOKIE_NAME

logger = __import__('logging').getLogger(__name__)


def set_research_status(user, allow_research):
    """
    For a user, set the research status.
    """
    research_status = IUserResearchStatus(user)
    if IBroken.providedBy(research_status):
        delete_research_status(user)
        research_status = IUserResearchStatus(user)
    research_status.updateLastMod()
    research_status.modified = datetime.utcnow()
    research_status.allow_research = allow_research
    notify(UserResearchStatusEvent(user, allow_research))


def _get_session_id_from_val(val):
        if val is None:
            return None

        try:
            result = int( val )
        except ValueError:
            # Shouldn't get here.
            logger.warn('Received analytics session id that is not an int (%s)',
                        val)
            result = None
        return result


def _get_header_id(request):
    val = request.headers.get(ANALYTICS_SESSION_HEADER)
    return _get_session_id_from_val(val)


def _get_cookie_id(request):
    val = request.cookies.get(ANALYTICS_SESSION_COOKIE_NAME)
    return _get_session_id_from_val(val)


def get_session_id_from_request(request):
    """
    Returns the int analytics session_id, if available, from this request.
    """
    result = None
    if request is not None:
        result = _get_header_id(request)

        if result is None:
            result = _get_cookie_id(request)
    return result
