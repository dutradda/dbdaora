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

- Same interface as [`sqlalchemy.orm.session.Session`](https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session) for the repository class

- Easy integration with other data sources

- Use redis data structure (like hashs, sets, etc) to store objects


## Requirements

Python 3.7+


## Instalation

```
$ pip install dataclassdb 
```


## Usage example

```python
from dataclassdb import DataSourceType, MainRepository
from dataclasses import dataclass


@dataclass
class Address:
  street: str


@dataclass
class Person:
    name: str
    age: int
    address: Address


mainRepository = MainRepository(
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
mainRepository.add(person, commit=True)

adressRepository = mainRepository.query(Address)
addresses = adressRepository.filter(street="john's street").all()

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
