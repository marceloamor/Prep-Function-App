# Prep Function App

The new single source of dynamic truths stored in Redis and the DB,
contains jobs to run on differing frequencies.

## Information

### Exchange helpers

All exchanges that require jobs run for them at any frequency or based
on any specific events should have this utility code stored here.

LME Forward prompt dates are calculated here, and static data will be
populated/cleaned with derivative contracts as required by configuration
options.

## Contributing

Simply clone into a project directory, install and run unit tests:

```sh
poetry install
poetry run pytest
```

This should ensure everything required is ready to go.

Note: you may need to manually install some other project dependencies such
as Azure's CLI and Azurite tools.
