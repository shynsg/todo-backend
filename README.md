# Todo Backend

Simple Express API for the Kubernetes capstone.

## Local

```bash
npm install
DATABASE_URL=postgres://postgres:postgres@localhost:5432/app npm run migrate
DATABASE_URL=postgres://postgres:postgres@localhost:5432/app npm start
```

## Docker

```bash
docker build -t todo-backend:local .
docker run --rm -p 3000:3000 \
  -e DATABASE_URL=postgres://postgres:postgres@host.docker.internal:5432/app \
  todo-backend:local
```

## API

```text
GET    /api/health
GET    /api/todos
POST   /api/todos
PATCH  /api/todos/:id
DELETE /api/todos/:id
GET    /api/stats
```
