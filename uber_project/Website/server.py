from flask import Flask, render_template


app = Flask(__name__, template_folder="templates", static_folder="statics")


@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


if __name__ == "__main__":
    print("Server is on ...")
    app.run(debug=True)
