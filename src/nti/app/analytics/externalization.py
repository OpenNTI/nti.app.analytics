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

from zope.interface.interfaces import IMethod

from nti.async.interfaces import IJob

from nti.externalization.externalization import to_external_object

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IInternalObjectExternalizer

ID = StandardExternalFields.ID

@component.adapter(IJob)
@interface.implementer(IInternalObjectExternalizer)
class AsyncJobExternalizer(object):

	def __init__(self, obj):
		self.job = obj

	def _ex_callable(self, x):
		return {
			'module': getattr(x, '__module__', None),
			'name': 	getattr(x, '__name__', None) \
					or	getattr(x, 'func_name', None)
		}

	def _ext_obj(self, x):
		try:
			if callable(x) or IMethod.providedBy(x):
				return self._ex_callable(x)
			else:
				return to_external_object(x)
		except Exception:
			return repr(x)

	def toExternalObject(self, **kwargs):
		result = LocatedExternalDict()
		job_call = self.job.callable
		job_args = self.job.args or ()
		job_kwargs = self.job.kwargs or dict()
		result[ID] = self.job.id
		result['status'] = self.job.status
		result['callable'] = self._ex_callable(job_call)
		result['args'] = [self._ext_obj(x) for x in job_args]
		result['kwargs'] = {x:self._ext_obj(y) for x,y in job_kwargs.items()}
		result['error'] = repr(self.job.error) if self.job.error else None
		return result

def to_external_job(job, **kwargs):
	return AsyncJobExternalizer(job).toExternalObject(**kwargs)
