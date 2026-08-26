from __future__ import annotations

from typing import Any


class EncryptionCoreMixin:
    def _encryption_request(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST an encryption/decryption request and normalize the response.

        Shared by ``encrypt`` and ``decrypt``, which differ only in the
        action word used for the endpoint and messages.

        Parameters
        ----------
        action : str
            ``"encrypt"`` or ``"decrypt"`` — selects
            ``POST /api/v1/encryption/{action}`` and is used in log/error
            messages.
        payload : dict[str, Any]
            Request body (typically includes a ``value`` field).

        Returns
        -------
        dict[str, Any]
            API response on success, or ``{"error": "..."}`` on failure.
        """
        if not isinstance(payload, dict):
            self.logger.error(f"Encryption {action} requires payload to be a dict.")
            return {"error": "payload must be a dictionary."}

        endpoint = f"/api/v1/encryption/{action}"
        self.logger.debug(f"POST {endpoint}")
        response = self.api_client.post(endpoint, data=payload)

        if response is None:
            self.logger.error(f"POST {endpoint} failed: No response received.")
            return {"error": f"No response received while performing {action}."}

        if not response.ok:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text if response else "No response text available."
            self.logger.error(f"POST {endpoint} failed. Error: {error_message}")
            return {"error": f"Failed to {action}. {error_message}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully completed encryption {action}.")
        return result

    def encrypt(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Encrypt a value using the Sisense encryption service.

        Sends ``POST /api/v1/encryption/encrypt``. Use when preparing connection
        parameters or keys for cross-server datamodel import.

        Parameters
        ----------
        payload : dict[str, Any]
            Encryption request body (typically includes a ``value`` field with
            the plaintext to encrypt).

        Returns
        -------
        dict[str, Any]
            Encryption response from the API, or ``{"error": "..."}`` on
            failure.
        """
        return self._encryption_request("encrypt", payload)

    def decrypt(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Decrypt a value using the Sisense encryption service.

        Sends ``POST /api/v1/encryption/decrypt``. Use when reading encrypted
        connection parameters from exported datamodels.

        Parameters
        ----------
        payload : dict[str, Any]
            Decryption request body (typically includes a ``value`` field with
            the ciphertext to decrypt).

        Returns
        -------
        dict[str, Any]
            Decryption response from the API, or ``{"error": "..."}`` on
            failure.
        """
        return self._encryption_request("decrypt", payload)
