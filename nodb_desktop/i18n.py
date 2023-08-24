

class Translator:

    def __init__(self):
        self.language_map = {
            'en': {
                'username': 'Username',
                'password': 'Password',
                'login_title': 'Login',
                'login': 'Login',
                'app_error': 'Application Error',
                'request_error': 'Request Error',
                'malformed_response': 'Server returned an invalid response',
                'window_title': 'CNODC Quality Control',
                'english': 'English',
                'success': 'Success',
                'login_success': 'Welcome',
                'session_expired': 'Session Expired',
                'session_expired_long': 'Your session has expired, please login again to continue',
                'request_info': 'Sending request to {endpoint}',
                "renewing_session": "Automatically renewing session...",
                "session_renewed": "Session renewed",
                "login_success_name": "Logged in as {username}"
            },
            'fr': {
                'username': 'Nom d\'utilisateur',
                'password': 'Mot de passe',
                'login_title': 'Connexioner',
                'login': 'Connexioner',
                'window_title': 'QC pour CNDOC',
                'french': 'Français',
            },
            'und': {
                'language_title': "Bienvenue | Welcome"
            }
        }

    def translate(self, language: str, resource_name: str):
        if language in self.language_map and resource_name in self.language_map[language]:
            return self.language_map[language][resource_name]
        return resource_name
