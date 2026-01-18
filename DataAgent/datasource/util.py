
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Util.strxor import strxor
import base64


def decrypt(encrypted_text: str, key: str = 'xingchenhuisoupd', iv: str = 'abcdef0123456789') -> str:
    """
    AES-CBC 解密，填充 PKCS7，输入 Base64 密文
    :param encrypted_text: Base64 编码的密文
    :param key: 16 字节密钥
    :param iv: 16 字节初始化向量
    :return: 明文字符串
    """

    encrypted_data = base64.b64decode(encrypted_text)
    cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv=iv.encode('utf-8'))
    decrypted_padded = cipher.decrypt(encrypted_data)
    decrypted = unpad(decrypted_padded, AES.block_size)
    return decrypted.decode('utf-8')

