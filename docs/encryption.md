Encryption Class Documentation
==============================

The `Encryption` class wraps Sisense encryption endpoints used when exporting or importing datamodel connection parameters across environments.

* * * * *

Class: `Encryption`
-------------------

### `__init__(self, api_client=None, debug=False)`

Initializes the `Encryption` class.

**Parameters:**

- `api_client` (SisenseClient, optional): An existing authenticated client.
- `debug` (bool): Enable debug logging when creating a new client.

* * * * *

### `encrypt(payload)`

Encrypts a value via `POST /api/v1/encryption/encrypt`.

**Parameters:**

- `payload` (dict): Request body (typically includes a `value` field with plaintext).

**Returns:**

- `dict`: API response on success, or `{"error": "..."}` on failure.

* * * * *

### `decrypt(payload)`

Decrypts a value via `POST /api/v1/encryption/decrypt`.

**Parameters:**

- `payload` (dict): Request body (typically includes a `value` field with ciphertext).

**Returns:**

- `dict`: API response on success, or `{"error": "..."}` on failure.
