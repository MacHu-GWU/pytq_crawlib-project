#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Sanhe's private crawler tool.
"""

__version__ = "0.0.2"
__short_description__ = ""
__license__ = "MIT"
__author__ = "Sanhe Hu"
__author__ = "Sanhe Hu"
__author_email__ = "husanhe@gmail.com"
__maintainer__ = "Sanhe Hu"
__maintainer_email__ = "husanhe@gmail.com"
__github_username__ = "MacHu-GWU"

try:
    from .scheduler import OneToOne, OneToMany
except:  # pragma: no cover
    pass
