module.exports = {
  apps: [{
    name: "the-sipae-api",
    // We explicitly point to the python executable inside the virtual environment
    // This assumes you created the venv inside the project folder: python3 -m venv venv
    script: "./venv/bin/python",
    args: "-m uvicorn main:app --host 0.0.0.0 --port 8000",
    interpreter: "none",
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: "500M",
    env: {
      NODE_ENV: "production",
    }
  }]
}