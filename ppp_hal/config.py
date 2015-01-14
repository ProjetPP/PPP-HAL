"""Configuration module."""
import os
import json
import logging
from ppp_libmodule.config import Config as BaseConfig
from ppp_libmodule.exceptions import InvalidConfig

class Config(BaseConfig):
    __slots__ = ('apis', 'memcached_servers', 'memcached_timeout')
    config_path_variable = 'PPP_HAL_CONFIG'
    
    def parse_config(self, data):
        self.apis = data['apis']
        self.memcached_servers = data['memcached']['servers']
        self.memcached_timeout = data['memcached']['timeout']
