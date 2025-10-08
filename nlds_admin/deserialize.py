from nlds_admin.rabbit import message_keys as MSG
import json
import zlib
import base64


def deserialize(body: str) -> dict:
    """Deserialize the message body by calling JSON loads and decompressing the
    message if necessary."""
    body_dict = json.loads(body)
    # check whether the DATA section is serialized
    if MSG.COMPRESS in body_dict[MSG.DETAILS] and body_dict[MSG.DETAILS][MSG.COMPRESS]:
        # data is in a b64 encoded ascii string - need to convert to bytes (in
        # ascii format before decompressing and loading into json
        try:
            byte_string = body_dict[MSG.DATA].encode("ascii")
        except AttributeError:
            raise RuntimeError(
                "DATA part of message was not compressed, despite compressed flag being"
                " set in message"
            )
        else:
            decompressed_string = zlib.decompress(base64.b64decode(byte_string))
            body_dict[MSG.DATA] = json.loads(decompressed_string)
            info = (
                f"Decompressing message, compressed size {len(byte_string)}, "
                f" actual size {len(decompressed_string)}"
            )
        # specify that the message is now decompressed, in case it gets passed through
        # deserialize again
        body_dict[MSG.DETAILS][MSG.COMPRESS] = False
    return body_dict
