from core.encoding import decode_response
from types import SimpleNamespace


class FakeResp(SimpleNamespace):
    
    @property
    def content(self):
        return self._content
    
    @property
    def headers(self):
        return self._headers
    
    @property
    def apparent_encoding(self):
        return self._apparent
    
    @property
    def url(self):
        return getattr(self, '_url', '')


# UTF-8 example
utf8_bytes = 'こんにちは'.encode('utf-8')
resp = FakeResp(**{
    '_content': utf8_bytes,
    '_headers': {
        'Content-Type': 'text/html; charset=utf-8'
    },
    '_apparent': 'utf-8',
    '_url': 'http://example/utf8'
})
print('utf8 ->', decode_response(resp))

# EUC-JP example
try:
    euc_bytes = 'こんにちは'.encode('euc_jp')
    resp2 = FakeResp(**{
        '_content': euc_bytes,
        '_headers': {
            'Content-Type': 'text/html; charset=EUC-JP'
        },
        '_apparent': 'euc_jp',
        '_url': 'http://example/euc'
    })
    print('euc  ->', decode_response(resp2))
except Exception as e:
    print('euc test skipped', e)
