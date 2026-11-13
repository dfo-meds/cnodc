Developer Guide
===============
Developers working on this project should follow this guide for best practices.

Setup
-----
You can use the code editor of your choice. To get your project setup:

 ::

    git clone github.com:dfo-meds/cnodc cnodc

    # create your project and virtual environment

    cd cnodc
    pip install -r requirements/.req.devs.txt

    pre-commit install

The pre-commit hooks installed take care of the following for you:

- Validation of YAML and TOML files
- Python validation
- Ensuring merge conflicts are resolved before commit
- Ensuring file send in a single new line
- Ensuring linux-style new lines
- Prevent you from committing to the main branch
- Remove trailing whitespace
- Run the bandit code report when you commit
- Run the ruff linter and formatter on commit
- Run the mypy type checker on commit


Coding
------

Python Version
``````````````
Code should be compatible with Python 3.14.

Naming
``````
* Functions, methods, variables, and attributes should be named using ``snake_case``
* Classes should be named using ``ProperCase``
* In class names, acronyms should keep their full uppercase version, e.g. ``HTTPError`` not ``HttpError``
* Generally, name things descriptively but succinctly and consistently

Comments
````````
* Inline comments should be done using ``# comment``
* Add comments to explain why something is done
* Let your code explain how it is done

Type Hinting
````````````
* Type hints are mandatory except where the type is ``typing.Any``


Documentation
-------------
Code should be documented using reStructuredText format and be compatible with Sphinx.
Python developers should follow PEP 8 and PEP 257 in writing documentation.

* A single-line docblock should start and end on one line like so: ``"""Calculate the square root."""``
* A multi-line docblock should start with the opening quotes and the first line. The closing quotes should be on their own line ::

    """Calculate the square root.

    Uses the Newton method
    """

* Multi-line docblocks should have consistent indentation aligned with the first quotation mark.

Modules (and Packages)
``````````````````````
* Package documentation should be placed in ``__init__.py``
* Provide a brief description of the module

Classes
```````
* The first line should succinctly describe the object.
* Class documentation should include key details on how the class should be used.
* Add documentation as appropriate for the ``__init__()`` method here (e.g. parameters, raises, etc.)
* Subclasses should document differences from the parent class

Functions (and Methods)
```````````````````````
* First line should be an imperative tense sentence summarizing the behaviour, e.g. ``Calculate the square root.`` not
  ``Calculates the square root.``
* Functions which are obvious in their description can have just the first line.
* If the function has known side-effects, they should be clearly outlined
* Parameters should be documented with ``:param <type> <name>: <short description>``
* Optional parameters should include ``[default: <value>]`` at the end of their description
* Return value should be documented with ``:return: <short description>`` and ``:rtype: <type>``
* If the function can raise an exception, document it with ``:raises <type>: <condition in which this is raised>``.
  Include any functions that this one calls.
* If the function accepts ``**kwargs`` or ``*args``, the expected types (and names for ``**kwargs``) should be documented.
* Private functions should include ``:meta private:``
* Public functions should include ``:meta public:``
* You do not need to document well-known dunder methods like ``__init__()`` unless you need to clarify something

Class and Module Attributes
```````````````````````````
* These should be documented with """<description>""" immediately after the member.


Git Workflow
------------
When working on new code, developers should:

1. Divide their work into small atomic pieces that can be reviewed and committed without breaking the software.
2. Make a new branch for their code, branching from ``main``.
3. When complete, follow the checklist below and then open a pull request.
4. Open a pull request in GitHub to merge their branch with ``main``

The lead developer will then either approve and ``merge commit`` their work, or reject it with updates that are required.

Branch Names
````````````
Branch names should be in the form ``<type>/<short-description>``, where type is one of:

* feature: For a new feature
* fix: For a bugfix
* hotfix: For an urgent bug fix
* chore: For routine maintenance work
* docs: For documentation updates

Pre-Pull Request Checklist
``````````````````````````

.. attention::

   If you wrote your code with AI, you still need to review it to ensure it works properly
   and conforms with our best practices. Do not submit unreviewed pull requests.

1. Run ``git rebase main`` to ensure you have merged any changes to main since you started
2. Run ``bandit -r src`` and address issues (high severity issues must be addressed, medium issues should be addressed - ``nosec`` flags should be used sparingly and only where an issue is clearly a false positive)
2. Run ``python -m tests -C -L -I`` to ensure all the tests have passed (you can add ``-f`` to fail quickly) and that the code coverage is acceptable (minor coverage gaps where code is hard to test are acceptable).
3. Run ``ruff check --fix`` and review any errors to resolve (you can prioritize readability over some errors, especially for ``SIM103`` and ``SIM102``).
4. Run ``ruff format`` to format your code properly
5. Open your pull request

Commit Message Template
```````````````````````
Commit messages should be in the format ::

    <type>(<scope>): <short summary>

    <additional content, if necessary>


The ``<type>`` should be one of:

* feature: New feature
* fix: Bug fix
* docs: Documentation update
* style: Formatting only
* refactor: Restructuring (no new features or fixes)
* test: Test changes only
* chore: Dependency updates or similar maintenance task
* perf: Performance improvement

Pull Request Template
`````````````````````
 ::

    ## Change
    <bulletpoints explaining the change briefly>

    ## Why
    <motivation behind change, include link to any issues>


The response should be provided within one business day and include lines of the format ``<type>: <note>`` where type is one of:

* nit: Optional fix, not necessary, but good suggestion
* blocker: Must fix before acceptance
* q: Question to the submitter


Chores
------
Senior developer responsibilities:

* ``pip-audit`` should be run regularly against all Python dependencies
* Docker container images should be checked regularly for updates (integrate ``trivy``)
* TODO: we should enforce these better with pre-commit hooks and CI/CD workflow


References
----------
* https://www.sphinx-doc.org/en/master/usage/domains/python.html
* https://peps.python.org/pep-0257/
* https://peps.python.org/pep-0287/
