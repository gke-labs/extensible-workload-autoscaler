from flask import Flask

app = Flask(__name__)

def fibonacci(n):
    if n < 0:
        raise ValueError("Input must be a non-negative integer")
    elif n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        # Deliberately use the less efficient recursive implementation to burn
        # CPU cycles; a more efficient iterative implementation (commented out
        # below) could be used instead.
        return fibonacci(n-1) + fibonacci(n-2)
        #a, b = 0, 1
        #for _ in range(2, n + 1):
        #    a, b = b, a + b
        #return b

@app.route('/')
def handler():
    fib_30 = fibonacci(30)
    return f"The 30th fibonacci number is {fib_30}"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
