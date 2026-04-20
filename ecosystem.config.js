const path = require("path");

const appDir = __dirname;
const appName = process.env.PM2_APP_NAME || "the-sipae-api";
const port = process.env.PORT || "18100";

module.exports = {
  apps: [
    {
      name: appName,
      cwd: appDir,
      script: path.join(appDir, "venv", "bin", "python"),
      args: `-m uvicorn main:app --host 0.0.0.0 --port ${port}`,
      interpreter: "none",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "500M",
      env: {
        NODE_ENV: "production",
        PM2_APP_NAME: appName,
        PORT: port
      }
    }
  ]
};