# Contributing to pysisense

We want to make contributing to this project as easy and transparent as possible.
This guide will provide you with information on how to contribute code changes to the project.
Please make sure to read this guide carefully before submitting any contributions.

To get an overview of the project, read the [README](README.md).
Here are some resources to help you get started:

- [PEP 8 Style Guide](https://www.python.org/dev/peps/pep-0008/)
- [UV Package Manager](https://docs.astral.sh/uv/)
- [Pytest Documentation](https://docs.pytest.org/)

## Table of Contents

1. [Coding Conventions](#coding-conventions)
2. [Development Flow](#development-flow)
3. [Testing](#testing)
4. [Documentation Guidelines](#documentation-guidelines)

## Coding Conventions

### General Guidelines

- We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide for Python code.
  - We use [Ruff](https://docs.astral.sh/ruff/) for linting and import sorting.
- Code should be clear, maintainable, and follow Python best practices.
  - Avoid obscure abbreviations or complex one-liners.
  - Use meaningful variable and function names.
  - Add comments for non-obvious logic.

### Naming Conventions

- **Modules and files**: Use lowercase with underscores (snake_case): `access_management.py`, `data_model.py`
- **Classes**: Use PascalCase: `SisenseClient`, `DataModel`, `AccessManagement`
- **Functions and methods**: Use lowercase with underscores (snake_case): `get_user()`, `create_dashboard()`
- **Constants**: Use uppercase with underscores: `MAX_RETRIES`, `DEFAULT_TIMEOUT`
- **Private attributes/methods**: Prefix with underscore: `_internal_method()`, `_private_var`

## Development Flow

### Setting Up Your Environment

1. **Clone the repository**:
   ```bash
   git clone https://github.com/sisense/pysisense.git
   cd pysisense
   ```

2. **Install UV package manager** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Create a virtual environment and install dependencies**:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv pip install -e ".[dev]"
   ```

### General Guidelines

When making contributions to the project, please follow these steps:

1. Create a new branch from the **dev** branch.
   Branch names should be descriptive and informative:
   - Use lowercase with hyphens: `your-name/add-user-management` or `your-name/dashboard-loading-issue`
   - Include a brief description of the change

2. Make your changes to the codebase. Please ensure that your changes are:
   - Well-tested with passing unit tests
   - Follow PEP 8 style guidelines checked by Ruff
   - Documented with appropriate docstrings

3. Commit your changes with clear, descriptive commit messages following the structure below.

4. Create a pull request (PR) for your branch following the PR structure below.

5. After your PR is approved and all checks pass, merge it into the main branch.

### Git Commit Message Structure

We follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) guidelines
for writing commit messages.

The commit message should be structured as follows:

```text
<type>(<scope>): <description>

[optional body describing the change in more detail]

[optional footer(s)]
```

Notable requirements:

- The following `<type>` values are supported:
  - `feat`: A new feature
  - `fix`: A bug fix
  - `refactor`: A code change that neither fixes a bug nor adds a feature
  - `docs`: Documentation only changes
  - `test`: Adding missing tests or correcting existing tests
  - `chore`: Other changes that don't modify src or test files (dependencies, build, etc.)
  - `perf`: A code change that improves performance

- Use present tense imperative-style verbs in the description: `add user validation` instead of `added user validation`
- Keep the description concise and informative

### Pull Request (PR) Structure

The PR structure should align with your commit message structure.
Specifically, the PR title should follow the Conventional Commits format.

- Make your PR title clear and descriptive following the commit message format
- Provide a detailed description of your changes in the PR body:
  - What problem does this solve?
  - What changes were made?
  - How can reviewers test your changes?
- Reference any related issues using GitHub's syntax: `Closes #123`
- Add appropriate labels to your PR (e.g., `bug`, `feature`, `documentation`)

Example PR title: `feat(access_management): add role-based access control`

## Testing

We use [Pytest](https://docs.pytest.org/) as the testing framework for unit and integration tests.

### Unit Testing

- Test files are located in the `tests/unit/` directory and follow the naming convention `test_*.py`
- Each test file should test a single module or component from the main package
- Tests should be isolated, deterministic, and run without external dependencies when possible
- Use fixtures for common test setup and teardown
- Aim for high test coverage, particularly for critical functionality

**Running unit tests:**

```bash
# Run all unit tests
pytest tests/unit/

# Run specific test file
pytest tests/unit/test_module.py

# Run with coverage report
pytest tests/unit/ --cov=pysisense --cov-report=html
```

### Integration Testing

- Integration tests are located in the `tests/integration/` directory
- These tests verify that different components work together correctly
- Integration tests may require a Sisense instance or mock API responses
- Clearly document any external dependencies or setup requirements

**Running integration tests:**

```bash
# Run all integration tests
pytest tests/integration/

# Run specific integration test
pytest tests/integration/wellcheck/test_dashboard_structure.py
```

### Test Best Practices

- Write tests that are independent of execution order
- Use descriptive test names that clearly indicate what is being tested: `test_user_creation_with_valid_data`
- Mock external API calls to avoid dependencies on live services
- Include both positive and negative test cases
- Keep tests focused and avoid testing multiple unrelated things in one test
- Use `pytest.mark` decorators for organizing tests (e.g., `@pytest.mark.integration`, `@pytest.mark.slow`)

### Code Quality Checks

Before committing your code, ensure it passes all quality checks:

```bash
# Lint code with Ruff
ruff check pysisense/ tests/

# Run all tests
pytest tests/
```

## Documentation Guidelines

Python code should be well-documented with clear docstrings following the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings). Documentation is generated using pydocs.

### Docstring Style

We use Google-style docstrings for all public functions, classes, and methods. This style is compatible with documentation generation tools like Sphinx.

#### Module-Level Docstring

Every module should start with a module-level docstring:

```python
"""Access management utilities for Sisense API.

This module provides functions for managing user access and permissions
within Sisense dashboards and data models.
"""
```

#### Function and Method Docstrings

```python
def create_user(name: str, email: str, role: str = "viewer") -> dict:
    """Create a new user in Sisense.
    
    Args:
        name: The full name of the user.
        email: The email address of the user.
        role: The role to assign to the user. Defaults to "viewer".
            Valid roles are: "viewer", "editor", "admin".
    
    Returns:
        A dictionary containing the created user's information.
        
    Raises:
        ValueError: If email format is invalid.
        APIError: If the API request fails.
        
    Example:
        >>> user = create_user("John Doe", "john@example.com", role="editor")
        >>> print(user['id'])
    """
    pass
```

#### Class Docstrings

```python
class SisenseClient:
    """A client for interacting with the Sisense API.
    
    This class provides methods for authentication, data retrieval, and
    resource management within Sisense.
    
    Attributes:
        domain (str): The Sisense instance domain.
        api_token (str): The API token for authentication.
        ssl_enabled (bool): Whether to use SSL for API calls.
    
    Example:
        >>> client = SisenseClient(domain="example.com", api_token="token", ssl_enabled=True)
        >>> user = client.get_user("user_id")
    """
    pass
```

### Documentation Best Practices

- **Be concise but complete**: Provide enough detail for users to understand how to use your code.
- **Use type hints**: Always include type hints for function parameters and return values.
- **Document exceptions**: Clearly specify which exceptions can be raised and under what conditions.
- **Include examples**: For complex functions, include usage examples in the docstring.
- **Update documentation**: When you modify code, update the corresponding docstrings.
- **Avoid obvious docstrings**: Don't write docstrings that just repeat the code.
  
  Bad: `def get_name(self): """Get the name.""" return self.name`
  
  Good: `def get_name(self) -> str: """Get the user's full name as displayed in the system.""" return self.name`

### README and Examples

- Keep the main [README](README.md) up-to-date with installation and basic usage instructions.
- Provide practical examples in the [examples/](examples/) directory for common use cases.
- Update [docs/](docs/) with detailed documentation for complex features.

## Conclusion

Thank you for considering contributing to pysisense! We appreciate your contributions and look forward to working with you!

If you have any questions or need clarification on any part of this guide, please open an issue or reach out to the maintainers.

Happy contributing! 🎉