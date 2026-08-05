# Carkie convoy service

Run the API from this directory:

```powershell
.\venv\Scripts\uvicorn.exe main:app --host 0.0.0.0 --port 8000 --reload
```

The Expo app automatically uses the same computer address as the Metro server. To point it at a deployed API instead, set `EXPO_PUBLIC_API_URL` before starting Expo.
