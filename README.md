# dataclassdb
Generates SQL and NoSQL Database Models from @dataclass

## Instalation
```
$ pip install dataclassdb 
```

## Usage example
```python
from dataclassdb import RepositoriesFactory
from dataclasses import dataclass


@dataclass
class Address:
  street: str


@dataclass
class Person:
    name: str
    age: int
    address: Address


adressRepository, personRepostiory = RepositoriesFactory.make(
    Address,
    Person,
    engine=sqlalchemy,
    engine_params=dict(
        db_url='sqlite://',
        create=True,
        backref_all=True,
    )
)

person = Person(
    name='John',
    age=78,
    address=Address("john's street")
)
personRepository.add(person)

addresses = addressRepository.filter(street="john's street").all()

print(addresses)

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
