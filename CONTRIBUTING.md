# Contributing to atio

🎉 Thank you for your interest in atio!
<br>
Below is a basic guide to help you contribute to the project.

---

## Development Environment Setup

1.  **Fork the repository and clone it:**
    ```bash
    git clone [https://github.com/](https://github.com/)<your-username>/atio.git
    cd atio
    ```

2.  **Install development dependencies:**
    ```bash
    pip install -e '.[dev]'
    ```

3.  **Run the tests:**
    ```bash
    pytest
    ```
    > **Note**: All Pull Requests must pass the tests.

## Code Style

We use `black` and `isort` to maintain consistent code style.
<br>
Please run the following commands before committing:
```bash
black .
isort .
Pull Request Guidelines
Make sure your branch is up-to-date with main.

Include clear commit messages.

Follow the code style and run tests before submitting.

Open a Pull Request describing what changes you made and why.

Reporting Issues
If you find a bug or want to suggest a feature, please use our Issue Templates to create a report.

Bug Report

Feature Request

Code of Conduct
All contributors are expected to follow our Code of Conduct.
<br>
Please be respectful, inclusive, and constructive in all interactions.