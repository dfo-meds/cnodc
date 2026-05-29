import base64
import json
import pathlib
import typing as t
import uuid

from cryptography.fernet import Fernet, InvalidToken
from autoinject import injector
import os
from medsutil.exceptions import CodedError
from medsutil.storage import StorageController
import medsutil.datadict as dd

from oceanvault.policies import PolicyGroup, AccessType


class VaultError(CodedError): CODE_SPACE = 'VAULT'


class BaseSecret(dd.DataDictModifiedTracker):

    _vault: SecretVault

    path: str = dd.p_str(required=True, readonly=True)

    def set_vault(self, vault: SecretVault):
        self._vault = vault

    def check_access(self, pg: PolicyGroup, required_access: list[AccessType]):
        return pg.check(f'/secrets/{self.path}', required_access)

    def mark_modified(self, item: str):
        super().mark_modified(item)
        self._vault.mark_modified()


class KeyValueSecret(BaseSecret):
    value: bytes = dd.p_bytes_b64(required=True)


class SecretVault:

    storage: StorageController = None

    @injector.construct
    def __init__(self, secret_file: str | pathlib.Path):
        self._secret_file = secret_file
        self._master_key: bytes | None = None
        self._key_length = 4
        self._loaded: bool = False
        self._modified: bool = False
        self._secrets: dict[str, BaseSecret] = {}

    def set_secret(self, secret: BaseSecret):
        self._secrets[secret.path] = secret
        secret.set_vault(self)
        self.mark_modified()

    def get_secret(self, path: str) -> BaseSecret:
        return self._secrets[path.strip('/')]

    def mark_modified(self):
        self._modified = True

    def _require_loaded(self):
        if not self._loaded:
            raise VaultError('Vault must be opened first', 1000)

    def _require_not_loaded(self):
        if self._loaded:
            raise VaultError('Vault is already open', 1001)

    def _generate_master_key(self):
        self._master_key = Fernet.generate_key()
        self.mark_modified()

    def initialize_vault(self) -> bytes:
        self._require_not_loaded()
        self._generate_master_key()
        self._loaded = True
        self._save()
        return t.cast(bytes, self._master_key)

    def rotate_shared_key(self) -> bytes:
        self._require_loaded()
        self._generate_master_key()
        self._save()
        return t.cast(bytes, self._master_key)

    def open_vault(self, master_key: bytes):
        self._master_key = master_key
        self._open()

    def _open(self):
        self._require_not_loaded()
        fernet_shared = Fernet(t.cast(bytes, self._master_key))
        with self.storage.handle(self._secret_file) as sf:
            try:
                self._import_secret_info(fernet_shared.decrypt(sf.read_bytes()))
            except InvalidToken as ex:
                raise VaultError("Invalid real key", 2001) from ex

    def _import_secret_info(self, data: bytes):
        temp = {}
        data_dict = json.loads(data)
        if not isinstance(data_dict, dict):
            raise VaultError('Invalid secret info, not a dict', 2000)
        for key in data_dict:
            secret = BaseSecret.from_map(data_dict[key])
            temp[secret.path] = secret
            secret.set_vault(self)
        self._secrets = temp

    def save_vault(self):
        if self._modified:
            self._save()

    def _save(self):
        self._require_loaded()
        with self.storage.handle(self._secret_file) as sf:
            temp = sf.with_name(str(uuid.uuid4()))
            fernet_shared = Fernet(t.cast(bytes, self._master_key))
            with temp.open('wb') as h:
                h.write(fernet_shared.encrypt(self._export_secret_info()))
            temp.rename(sf)

    def _export_secret_info(self) -> bytes:
        raw = [
            self._secrets[x].export()
            for x in self._secrets
        ]
        return json.dumps(raw).encode('utf-8')

    @staticmethod
    def from_environ_b64(secret_key_env_name: str, secrets_file: str | pathlib.Path):
        sv = SecretVault(secrets_file)
        sv.open_vault(base64.urlsafe_b64decode(os.environ.get(secret_key_env_name, '')))
        return sv
