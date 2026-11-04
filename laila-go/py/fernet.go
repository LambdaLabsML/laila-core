package py

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"time"
)

// Fernet is cryptography.fernet.Fernet: AES-128-CBC + HMAC-SHA256 tokens,
// URL-safe base64 encoded, with a creation timestamp and optional TTL.
type Fernet struct {
	signingKey    []byte
	encryptionKey []byte
	now           func() time.Time
}

// Fernet_generate_key is Fernet.generate_key(): 32 random bytes, urlsafe-b64.
func Fernet_generate_key() []byte {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return []byte(base64.URLEncoding.EncodeToString(raw))
}

// New_Fernet is Fernet(key): key is urlsafe-base64 of 32 bytes (str or bytes).
func New_Fernet(key any) (*Fernet, error) {
	var k []byte
	switch x := key.(type) {
	case string:
		k = []byte(x)
	case []byte:
		k = x
	default:
		return nil, TypeError.New("key must be bytes or str")
	}
	raw, err := base64.URLEncoding.DecodeString(string(k))
	if err != nil {
		return nil, ValueError.New("Fernet key must be 32 url-safe base64-encoded bytes.")
	}
	if len(raw) != 32 {
		return nil, ValueError.New("Fernet key must be 32 url-safe base64-encoded bytes.")
	}
	return &Fernet{signingKey: raw[:16], encryptionKey: raw[16:], now: time.Now}, nil
}

// Encrypt is fernet.encrypt(data).
func (f *Fernet) Encrypt(data []byte) ([]byte, error) {
	iv := make([]byte, 16)
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}
	return f.Encrypt_at_time(data, f.now().Unix(), iv)
}

// Encrypt_at_time is fernet._encrypt_from_parts(data, current_time, iv).
func (f *Fernet) Encrypt_at_time(data []byte, currentTime int64, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(f.encryptionKey)
	if err != nil {
		return nil, err
	}
	padLen := 16 - len(data)%16
	padded := make([]byte, len(data)+padLen)
	copy(padded, data)
	for i := len(data); i < len(padded); i++ {
		padded[i] = byte(padLen)
	}
	ct := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ct, padded)
	basic := make([]byte, 0, 1+8+16+len(ct)+32)
	basic = append(basic, 0x80)
	basic = binary.BigEndian.AppendUint64(basic, uint64(currentTime))
	basic = append(basic, iv...)
	basic = append(basic, ct...)
	mac := hmac.New(sha256.New, f.signingKey)
	mac.Write(basic)
	basic = mac.Sum(basic)
	return []byte(base64.URLEncoding.EncodeToString(basic)), nil
}

// Decrypt is fernet.decrypt(token, ttl=None); ttl < 0 means no TTL.
func (f *Fernet) Decrypt(token any, ttl float64) ([]byte, error) {
	return f.Decrypt_at_time(token, ttl, f.now().Unix())
}

// Extract_timestamp is fernet.extract_timestamp(token).
func (f *Fernet) Extract_timestamp(token any) (int64, error) {
	data, err := f.verify(token)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(data[1:9])), nil
}

func (f *Fernet) verify(token any) ([]byte, error) {
	var t []byte
	switch x := token.(type) {
	case string:
		t = []byte(x)
	case []byte:
		t = x
	default:
		return nil, TypeError.New("token must be bytes or str")
	}
	data, err := base64.URLEncoding.DecodeString(string(t))
	if err != nil {
		return nil, InvalidToken.New()
	}
	if len(data) < 1+8+16+32 || data[0] != 0x80 {
		return nil, InvalidToken.New()
	}
	body, sig := data[:len(data)-32], data[len(data)-32:]
	mac := hmac.New(sha256.New, f.signingKey)
	mac.Write(body)
	if !hmac.Equal(mac.Sum(nil), sig) {
		return nil, InvalidToken.New()
	}
	return data, nil
}

// Decrypt_at_time is fernet.decrypt_at_time(token, ttl, current_time).
func (f *Fernet) Decrypt_at_time(token any, ttl float64, currentTime int64) ([]byte, error) {
	data, err := f.verify(token)
	if err != nil {
		return nil, err
	}
	ts := int64(binary.BigEndian.Uint64(data[1:9]))
	if ttl >= 0 {
		if ts+int64(ttl) < currentTime {
			return nil, InvalidToken.New()
		}
	}
	if currentTime+60 < ts {
		return nil, InvalidToken.New()
	}
	iv := data[9:25]
	ct := data[25 : len(data)-32]
	if len(ct)%16 != 0 || len(ct) == 0 {
		return nil, InvalidToken.New()
	}
	block, err := aes.NewCipher(f.encryptionKey)
	if err != nil {
		return nil, err
	}
	pt := make([]byte, len(ct))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(pt, ct)
	padLen := int(pt[len(pt)-1])
	if padLen < 1 || padLen > 16 || padLen > len(pt) {
		return nil, InvalidToken.New()
	}
	for _, b := range pt[len(pt)-padLen:] {
		if int(b) != padLen {
			return nil, InvalidToken.New()
		}
	}
	return pt[:len(pt)-padLen], nil
}
