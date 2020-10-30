import codecs
from setuptools import setup, find_packages

entry_points = {
    'console_scripts': [
        "nti_analytics_processor = nti.app.analytics.scripts.constructor:main",
        "nti_analytics_fail_processor = nti.app.analytics.scripts.failure_processor:main",
    ],
    "z3c.autoinclude.plugin": [
        'target = nti.app',
    ],
}

TESTS_REQUIRE = [
    'nti.app.products.courseware',
    'nti.app.testing',
    'nti.testing',
    'zope.dottedname',
    'zope.testrunner',
    'nti.fakestatsd'
]


def _read(fname):
    with codecs.open(fname, encoding='utf-8') as f:
        return f.read()


setup(
    name='nti.app.analytics',
    version=_read('version.txt').strip(),
    author='Josh Zuech',
    author_email='josh.zuech@nextthought.com',
    description="NTI Analytics App",
    long_description=_read('README.rst'),
    license='Apache',
    keywords='pyramid analytics',
    classifiers=[
        'Framework :: Zope',
        'Framework :: Pyramid',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    url="https://github.com/NextThought/nti.app.analytics",
    zip_safe=True,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    namespace_packages=['nti', 'nti.app'],
    tests_require=TESTS_REQUIRE,
    install_requires=[
        'setuptools',
        'nti.app.asynchronous',
        'nti.app.products.courseware_ims',
        'nti.analytics',
        'nti.assessment',
        'nti.common',
        'nti.contentlibrary',
        'nti.contenttypes.courses',
        'nti.externalization',
        'nti.links',
        'nti.ntiids',
        'nti.site',
        'perfmetrics',
        'pyramid',
        'requests',
        'six',
        'z3c.autoinclude',
        'ZODB',
        'zope.cachedescriptors',
        'zope.component',
        'zope.event',
        'zope.i18nmessageid',
        'zope.interface',
        'zope.intid',
        'zope.location',
        'zope.schema',
        'zope.security',
        'zope.traversing',
    ],
    extras_require={
        'test': TESTS_REQUIRE,
        'docs': [
            'Sphinx',
            'repoze.sphinx.autointerface',
            'sphinx_rtd_theme',
        ],
    },
    entry_points=entry_points,
)
