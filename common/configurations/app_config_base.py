from typing import Dict, Any

class AppConfigBase:
    """app_config store basic data from app_config.json file

    Example:    
        app_config = AppConfigBase({
            "application_mode": "PRODUCTION",
            "config_file": "appconfig.PROD.json"
        })
    """    
    application_mode: str
    config_file: str
    debug_mode: bool = False

    def __init__(self, config_dict: Dict[str, Any]):
        self._config = config_dict
        self.application_mode = config_dict.get("application_mode", "PRODUCTION")
        self.config_file = config_dict.get("config_file", "appconfig.PROD.json")

    def __getattr__(self, name: str) -> Any:
        if name in self._config:
            return self._config[name]
        raise AttributeError(f"'AppConfig' object has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any) -> None:
        if name in ['_config']:
            super().__setattr__(name, value)            
        else:
            self._config[name] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)
    
    # this method to allow dictionary-like access 
    def __getitem__(self, key):        
        if isinstance(key, tuple):
            value = self._config
            for k in key:
                value = value[k]
            return value
        return self._config[key]
    
    def __setitem__(self, key, value):
        if isinstance(key, tuple):
            d = self._config
            for k in key[:-1]:
                if k not in d:
                    d[k] = {}
                elif not isinstance(d[k], dict):
                    d[k] = {}
                d = d[k]
            d[key[-1]] = value
        else:
            self._config[key] = value
    
    def __repr__(self) -> str:
        return f"AppConfig({self._config})"

