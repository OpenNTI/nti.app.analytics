#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from datetime import datetime

from ZODB.interfaces import IBroken

from zope.event import notify

from nti.analytics.interfaces import IUserResearchStatus

from nti.analytics.model import delete_research_status
from nti.analytics.model import UserResearchStatusEvent

def set_research_status( user, allow_research ):
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
