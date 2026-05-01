import base64
import json
import pathlib
import typing as t
import uuid

from cryptography.fernet import Fernet, InvalidToken
import shamirs
from autoinject import injector

from medsutil.exceptions import CodedError
from medsutil.storage import StorageController
from medsutil.vlq import vlq_encode, vlq_decode
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
    def __init__(self,
                 secret_file: str | pathlib.Path,
                 modulus: int = (2 * 10) - 1,
                 threshold: int = 2):
        self._secret_file = secret_file
        self._modulus = modulus
        self._threshold = threshold
        self._shared_key: bytes | None = None
        self._real_key: bytes | None = None
        self._key_length = 4
        self._loaded: bool = False
        self._modified: bool = False
        self._secrets: dict[str, BaseSecret] = {}

    def set_secret(self, secret: BaseSecret):
        self._secrets[secret.path] = secret
        secret.set_vault(self)

    def get_secret(self, path: str) -> BaseSecret:
        return self._secrets[path.strip('/')]

    def mark_modified(self):
        self._modified = True

    def create_vault(self, quantity: int = 5) -> list[bytes]:
        self._require_not_loaded()
        self._generate_real_key()
        self._generate_shared_key()
        keys = self._build_shared_keys(quantity)
        self._loaded = True
        self._save()
        return keys

    def rotate_main_key(self):
        self._require_loaded()
        self._generate_real_key()
        self._save()

    def rotate_shared_key(self, quantity: int = 5) -> list[bytes]:
        self._require_loaded()
        self._generate_shared_key()
        keys = self._build_shared_keys(quantity)
        self._save()
        return keys

    def _require_loaded(self):
        if not self._loaded:
            raise VaultError('Vault must be opened first', 1000)

    def _require_not_loaded(self):
        if self._loaded:
            raise VaultError('Vault is already open', 1001)

    def _generate_shared_key(self):
        self._shared_key = Fernet.generate_key()
        self._modified = True

    def _generate_real_key(self):
        self._real_key = Fernet.generate_key()
        self._modified = True

    def _build_shared_keys(self, quantity: int = 5) -> list[bytes]:
        shared_key_as_int = int.from_bytes(base64.urlsafe_b64decode(t.cast(bytes, self._shared_key)), byteorder='little')
        shares = shamirs.shares(shared_key_as_int, quantity=quantity, modulus=self._modulus, threshold=self._threshold)
        return list(x.to_bytes() for x in shares)

    def _decrypt_shared_key(self, shares: t.Sequence[bytes]):
        real_shares = [
            shamirs.share.from_bytes(x)
            for x in shares
        ]
        shared_key_as_int = shamirs.interpolate(real_shares, modulus=self._modulus, threshold=self._threshold)
        self._shared_key = base64.urlsafe_b64encode(int.to_bytes(self._key_length, shared_key_as_int, byteorder='little'))

    def open_vault(self, *shares):
        self._require_not_loaded()
        self._decrypt_shared_key(shares)
        fernet_shared = Fernet(t.cast(bytes, self._shared_key))
        with self.storage.handle(self._secret_file) as sf:
            data = sf.read_bytes()
            key_length, offset = vlq_decode(data)
            encrypted_key = data[offset:offset + key_length]
            encrypted_data = data[offset+key_length:]
            try:
                self._real_key = fernet_shared.decrypt(encrypted_key)
            except InvalidToken as ex:
                raise VaultError("Invalid shared key", 2000) from ex
            try:
                fernet_real = Fernet(t.cast(bytes, self._real_key))
                self._import_secret_info(fernet_real.decrypt(encrypted_data))
            except InvalidToken as ex:
                raise VaultError("Invalid real key", 2001) from ex

    def save_vault(self):
        self._require_loaded()
        if self._modified:
            self._save()

    def _save(self):
        with self.storage.handle(self._secret_file) as sf:
            temp = sf.with_name(str(uuid.uuid4()))
            fernet_real = Fernet(t.cast(bytes, self._real_key))
            fernet_shared = Fernet(t.cast(bytes, self._shared_key))
            encrypted_key = fernet_shared.encrypt(t.cast(bytes, self._real_key))
            encrypted_data = fernet_real.encrypt(self._export_secret_info())
            with temp.open('wb') as h:
                h.write(vlq_encode(len(encrypted_key)))
                h.write(encrypted_key)
                h.write(encrypted_data)
            temp.rename(sf)

    def _export_secret_info(self) -> bytes:
        raw = [
            self._secrets[x].export()
            for x in self._secrets
        ]
        return json.dumps(raw).encode('utf-8')

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
