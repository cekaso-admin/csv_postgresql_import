# ADR-006: API Key Authentication

**Status:** Accepted
**Date:** 2024-12-10

## Context

The CSV Import API needs authentication to:
1. Prevent unauthorized access to database connections (which contain credentials)
2. Protect project configurations from tampering
3. Secure import job execution
4. Allow integration with n8n and other automation tools

Options considered:
- **Simple API Key**: Single shared key, easy to implement
- **Supabase Auth**: JWT-based, user management built-in
- **OAuth2**: Industry standard, complex setup

## Decision

Use **Simple API Key authentication** for initial implementation.

### Implementation

1. **Environment Variable**: `API_KEY` stores the secret key
2. **Header**: `X-API-Key` carries the key in requests
3. **Scope**: All endpoints except `/health` and documentation (`/docs`, `/redoc`)

### Security Model

| Endpoint | Authentication |
|----------|----------------|
| `GET /health` | Public |
| `GET /docs` | Public |
| `GET /redoc` | Public |
| `GET /openapi.json` | Public |
| All other endpoints | Requires `X-API-Key` header |

### Error Responses

| Status | Condition |
|--------|-----------|
| 401 Unauthorized | No `X-API-Key` header provided |
| 403 Forbidden | Invalid API key |
| 500 Internal Server Error | `API_KEY` not configured on server |

## Configuration

```bash
# .env
# Generate with: python -c "import secrets; print(secrets.token_urlsafe(32))"
API_KEY=your-secure-api-key-here
```

## Usage

### curl

```bash
curl -H "X-API-Key: your-api-key" http://localhost:8000/projects
```

### n8n HTTP Request Node

1. Add header: `X-API-Key` = `{{$credentials.apiKey}}`
2. Or use "Header Auth" credential type

### Python requests

```python
import requests

headers = {"X-API-Key": "your-api-key"}
response = requests.get("http://localhost:8000/projects", headers=headers)
```

## Consequences

### Positive

- Simple to implement and understand
- Easy integration with n8n and automation tools
- No external dependencies (no auth server needed)
- Works with Swagger UI "Authorize" button

### Negative

- Single shared key (no per-user tracking)
- Key rotation requires updating all clients
- No built-in key expiration

### Future Considerations

- Add Supabase Auth when building frontend
- Consider API key rotation mechanism
- Add rate limiting per API key
- Log API key usage for auditing

## Alternatives Considered

### Supabase Auth

Would provide:
- JWT tokens with expiration
- User management via dashboard
- Row Level Security in database

Rejected because:
- More complex setup
- Overkill for n8n integration
- Can be added later when frontend is built

### OAuth2 / OpenID Connect

Would provide:
- Industry standard
- Multiple identity providers

Rejected because:
- Complex setup and maintenance
- Requires external identity provider
- Overkill for current use case
