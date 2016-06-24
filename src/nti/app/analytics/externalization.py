#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope import interface

from nti.async.interfaces import IJob

from nti.externalization.externalization import to_external_object

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IInternalObjectExternalizer

ID = StandardExternalFields.ID

@component.adapter(IJob)
@interface.implementer(IInternalObjectExternalizer)
class _AsyncJobExternalizer(object):

	def __init__(self, obj):
		self.job = obj

	def _ext_obj(self, x):
		try:
			return to_external_object(x)
		except Exception:
			return repr(x)

	def toExternalObject(self, **kwargs):
		result = LocatedExternalDict()
		args = self.job.args or ()
		job_call = self.job.callable
		kwargs = self.job.kwargs or dict()
		result[ID] = self.job.id
		result['status'] = self.job.status
		result['callable'] = {
			'module': getattr(job_call, '__module__', None),
			'name': 	getattr(job_call, '__name__', None) \
					or	getattr(job_call, 'func_name', None)
		}
		result['args'] = [self._ext_obj(x) for x in args or ()]
		result['kwargs'] = {x:self._ext_obj(y) for x,y in kwargs.values()}
		return result
