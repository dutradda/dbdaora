# dataclassdb

Generates ***SQL*** and ***NoSQL*** Database Models from @dataclass

---

**Documentation**: <a href="https://dutradda.github.io/dataclassdb" target="_blank">https://dutradda.github.io/dataclassdb</a>

**Source Code**: <a href="https://github.com/dutradda/dataclassdb" target="_blank">https://github.com/dutradda/dataclassdb</a>

---


## Key Features

- Fast start data modeling with persistence

- Supports from simple database schemas to complex one

- Integrate with:
    + `SQLALchemy`
    + `aioredis` (*soon*)
    + `google-datastore` (*soon*)
    + `mongodb` (*planned*)
    + `elasticsearch` (*planned*)
    + `aws-dynamodb` (*planned*)

- Same interface as `SQLAlchemy.Session` for the repositories classes

- Easy integration with other data sources

- Use redis data structure (like hashs, sets, lists, etc) to store objects


## Requirements

Python 3.7+


## Instalation

```
$ pip install dataclassdb 
```


## Usage example

```python
from dataclassdb import RepositoriesFactory, DataSourceType
from dataclasses import dataclass


@dataclass
class Address:
  street: str


@dataclass
class Person:
    name: str
    age: int
    address: Address


AdressRepository, PersonRepostiory = RepositoriesFactory.make(
    Address,
    Person,
    data_source=DataSourceType.RELATIONAL_SQLALCHEMY,
    data_source_args=dict(
        db_url='sqlite://',
        create=True,
        all_backref=True,
    )
)

person = Person(
    name='John',
    age=78,
    address=Address("john's street")
)
PersonRepository().add(person, commit=True)

addresses = AddressRepository().filter(street="john's street").all()

print(addresses)
```

```bash
[
  Address(
    street="john's street",
    _id=1,
    backrefs=Backrefs(
       person=[Person(age=78, name=John, _id=1)]
    )
  )
]
```

Detailed usage and documantion is in working process.
