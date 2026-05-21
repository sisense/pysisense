# Migration Chat (React)

React-based chat UI for the migration agent. Calls the Flask backend `/api/chat` endpoint.

## Setup

```bash
cd frontend/chat
npm install
```

## Development

```bash
npm run dev
```

Then open the main Flask app and go to **Chat** → "Open full React chat", or open `http://localhost:5001/chat/` (Flask must be running). For hot reload during development, run Vite dev server (`npm run dev`) and proxy API to Flask, or build and refresh.

## Build for production

```bash
npm run build
```

Output is in `frontend/chat/dist/`. Flask serves it at `/chat/` when the `dist` folder is present.

## PyInstaller

To include the React chat in the packaged app:

1. Build the chat: `cd frontend/chat && npm run build`
2. Ensure `frontend/chat/dist/` is included in the PyInstaller bundle (e.g. add `frontend/chat/dist` as a data directory in your `.spec` or build script).

At runtime, the exe serves the chat from the bundled `dist` files; no Node is required.
