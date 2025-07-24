package app.softnetwork.persistence.query

import app.softnetwork.elastic.client.ElasticClientApi
import app.softnetwork.elastic.persistence.query.{ElasticProvider, State2ElasticProcessorStream}
import app.softnetwork.persistence.person.message.PersonEvent
import app.softnetwork.persistence.person.model.Person
import app.softnetwork.persistence.person.query.PersonToExternalProcessorStream

trait PersonToElasticProcessorStream
    extends State2ElasticProcessorStream[Person, PersonEvent]
    with PersonToExternalProcessorStream
    with InMemoryJournalProvider
    with InMemoryOffsetProvider
    with ElasticProvider[Person] { _: ElasticClientApi => }
