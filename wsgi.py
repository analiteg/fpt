from forexpairtrading import app
from forexpairtrading import server

if __name__ == "__main__":
    app.run_server(port=5000, debug = True)