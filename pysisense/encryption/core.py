from __future__ import annotations

from typing import Any


class EncryptionCoreMixin:
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
        if not isinstance(payload, dict):
            self.logger.error("Encryption encrypt requires payload to be a dict.")
            return {"error": "payload must be a dictionary."}

        endpoint = "/api/v1/encryption/encrypt"
        self.logger.debug(f"POST {endpoint}")
        response = self.api_client.post(endpoint, data=payload)

        if response is None:
            self.logger.error(f"POST {endpoint} failed: No response received.")
            return {"error": "No response received while performing encrypt."}

        if not response.ok:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text if response else "No response text available."
            self.logger.error(f"POST {endpoint} failed. Error: {error_message}")
            return {"error": f"Failed to encrypt. {error_message}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info("Successfully completed encryption encrypt.")
        return result

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
        if not isinstance(payload, dict):
            self.logger.error("Encryption decrypt requires payload to be a dict.")
            return {"error": "payload must be a dictionary."}

        endpoint = "/api/v1/encryption/decrypt"
        self.logger.debug(f"POST {endpoint}")
        response = self.api_client.post(endpoint, data=payload)

        if response is None:
            self.logger.error(f"POST {endpoint} failed: No response received.")
            return {"error": "No response received while performing decrypt."}

        if not response.ok:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text if response else "No response text available."
            self.logger.error(f"POST {endpoint} failed. Error: {error_message}")
            return {"error": f"Failed to decrypt. {error_message}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info("Successfully completed encryption decrypt.")
        return result
