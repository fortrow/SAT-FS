from uuid import UUID
import unittest

from cxnfs import (
    AuthChallenge,
    AuthMechanism,
    AuthStart,
    ClientAuthenticator,
    ClientCredentials,
    PROOF_ALGORITHM,
    derive_password_proof,
    new_auth_challenge,
    prompt_client_credentials,
    verify_password_proof,
)


class CxnfsAuthTest(unittest.TestCase):
    def test_prompt_collects_username_and_hidden_password(self):
        prompts = []

        def input_fn(prompt):
            prompts.append(("input", prompt))
            return "trainer"

        def password_fn(prompt):
            prompts.append(("password", prompt))
            return "secret"

        credentials = prompt_client_credentials(input_fn, password_fn)

        self.assertEqual("trainer", credentials.username)
        self.assertEqual(b"secret", credentials.password_bytes())
        self.assertIn("password=<hidden>", repr(credentials))
        self.assertEqual([
            ("input", "CXNFS username: "),
            ("password", "CXNFS password: "),
        ], prompts)
        credentials.close()
        with self.assertRaisesRegex(ValueError, "closed"):
            credentials.password_bytes()

    def test_authenticator_sends_username_then_proof(self):
        challenge = AuthChallenge(
            UUID("00000000-0000-0000-0000-000000000001"),
            AuthMechanism.USERNAME_PASSWORD,
            "trainer",
            b"nonce-for-test",
            PROOF_ALGORITHM,
            2,
        )

        with ClientCredentials("trainer", "secret") as credentials:
            authenticator = ClientAuthenticator()
            start = authenticator.start(credentials)
            proof = authenticator.answer(challenge, credentials)

        self.assertEqual(AuthStart(AuthMechanism.USERNAME_PASSWORD, "trainer"), start)
        self.assertEqual(challenge.challenge_id, proof.challenge_id)
        self.assertEqual("trainer", proof.principal)
        self.assertTrue(verify_password_proof("secret", challenge.nonce, challenge.iterations, proof.proof))
        self.assertIn("proof=<hidden:", repr(proof))

    def test_rejects_mismatched_challenge_principal(self):
        challenge = new_auth_challenge("other", iterations=2)

        with ClientCredentials("trainer", "secret") as credentials:
            with self.assertRaisesRegex(ValueError, "principal"):
                ClientAuthenticator().answer(challenge, credentials)

    def test_pbkdf2_sha256_known_vector(self):
        proof = derive_password_proof("password", b"salt", iterations=1)

        self.assertEqual(
            bytes.fromhex("120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"),
            proof,
        )


if __name__ == "__main__":
    unittest.main()
