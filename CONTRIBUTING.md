---
# This is a YAML front matter block. 
# You can copy the 'content' value and paste it directly into a .md file.

content: |
  # Contributing to atio

  🎉 Thank you for your interest in atio!
  <br>
  Below is the basic guide for contributing to the project.

  ---

  ## Development Environment Setup

  1.  **Fork and clone the repository.**
      ```bash
      git clone [https://github.com/](https://github.com/)<your-username>/atio.git
      cd atio
      ```

  2.  **Install development dependencies.**
      ```bash
      pip install -e '.[dev]'
      ```

  3.  **Run the tests.**
      ```bash
      pytest
      ```
      > **Note**: All PRs must pass the tests.

  ---

  ## Code Style

  We use `black` and `isort` to unify our code style.
  <br>
  Please run the following commands before you commit:

  ```bash
  black .
  isort .