package mesosphere.marathon
package storage.migration

import akka.Done
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import mesosphere.AkkaUnitTest
import mesosphere.marathon.Protos.StorageVersion
import mesosphere.marathon.core.storage.backup.PersistentStoreBackup
import mesosphere.marathon.core.storage.store.PersistenceStore
import mesosphere.marathon.core.storage.store.impl.memory.InMemoryPersistenceStore
import mesosphere.marathon.state.RootGroup
import mesosphere.marathon.storage.{ InMem, StorageConfig }
import mesosphere.marathon.storage.migration.StorageVersions._
import mesosphere.marathon.storage.repository._
import mesosphere.marathon.test.Mockito
import org.scalatest.GivenWhenThen
import Migration.MigrationAction
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, Materializer }
import com.typesafe.config.ConfigValueFactory
import org.slf4j.{ Logger => SLF4JLogger }

import concurrent.duration._
import scala.concurrent.Future

class MigrationTest extends AkkaUnitTest with Mockito with GivenWhenThen {

  val newConfig = akkaConfig.withValue("migration.status-logging-interval", ConfigValueFactory.fromAnyRef("200 millis"))

  val newSystem: ActorSystem = ActorSystem(suiteName, newConfig)

  val m: Materializer = ActorMaterializer()(newSystem)

  private[this] def migration(
    persistenceStore: PersistenceStore[_, _, _] = new InMemoryPersistenceStore(),
    appRepository: AppRepository = mock[AppRepository],
    podRepository: PodRepository = mock[PodRepository],
    groupRepository: GroupRepository = mock[GroupRepository],
    deploymentRepository: DeploymentRepository = mock[DeploymentRepository],
    instanceRepository: InstanceRepository = mock[InstanceRepository],
    taskFailureRepository: TaskFailureRepository = mock[TaskFailureRepository],
    frameworkIdRepository: FrameworkIdRepository = mock[FrameworkIdRepository],
    configurationRepository: RuntimeConfigurationRepository = mock[RuntimeConfigurationRepository],
    backup: PersistentStoreBackup = mock[PersistentStoreBackup],
    serviceDefinitionRepository: ServiceDefinitionRepository = mock[ServiceDefinitionRepository],
    config: StorageConfig = InMem(1, Set.empty, None, None),
    newLogger: Logger = Logger(classOf[Migration]),
    fakeMigrations: List[MigrationAction] = List.empty): Migration = {

    // assume no runtime config is stored in repository
    configurationRepository.get() returns Future.successful(None)
    configurationRepository.store(any) returns Future.successful(Done)
    new Migration(Set.empty, None, "bridge-name", persistenceStore, appRepository, podRepository, groupRepository, deploymentRepository,
      instanceRepository, taskFailureRepository, frameworkIdRepository,
      serviceDefinitionRepository, configurationRepository, backup, config)(mat = m) {

      override val logger = newLogger

      override def migrations: List[(StorageVersion, () => Future[Any])] = if (fakeMigrations.nonEmpty) {
        fakeMigrations
      } else {
        super.migrations
      }
    }
  }

  val currentVersion: StorageVersion = StorageVersions.current

  "Migration" should {
    "be filterable by version" in {
      val migrate = migration()
      val all = migrate.migrations.filter(_._1 > StorageVersions(0, 0, 0)).sortBy(_._1)
      all should have size migrate.migrations.size.toLong

      val none = migrate.migrations.filter(_._1 > StorageVersions(Int.MaxValue, 0, 0, StorageVersion.StorageFormat.PERSISTENCE_STORE))
      none should be('empty)

      val some = migrate.migrations.filter(_._1 < StorageVersions(1, 5, 0, StorageVersion.StorageFormat.PERSISTENCE_STORE))
      some should have size 2 // we do have two migrations now, 1.4.2 and 1.4.6
    }

    "migrate on an empty database will set the storage version" in {
      val mockedStore = mock[PersistenceStore[_, _, _]]
      val migrate = migration(persistenceStore = mockedStore)

      mockedStore.storageVersion() returns Future.successful(None)
      mockedStore.setStorageVersion(any) returns Future.successful(Done)

      migrate.migrate()

      verify(mockedStore).storageVersion()
      verify(mockedStore).setStorageVersion(StorageVersions.current)
      noMoreInteractions(mockedStore)
    }

    "migrate on a database with the same version will do nothing" in {
      val mockedStore = mock[PersistenceStore[_, _, _]]
      val migrate = migration(persistenceStore = mockedStore)

      val currentPersistenceVersion =
        StorageVersions.current.toBuilder.setFormat(StorageVersion.StorageFormat.PERSISTENCE_STORE).build()
      mockedStore.storageVersion() returns Future.successful(Some(currentPersistenceVersion))
      migrate.migrate()

      verify(mockedStore).storageVersion()
      noMoreInteractions(mockedStore)
    }

    "migrate throws an error for early unsupported versions" in {
      val mockedStore = mock[PersistenceStore[_, _, _]]
      val migrate = migration(persistenceStore = mockedStore)
      val minVersion = migrate.minSupportedStorageVersion

      Given("An unsupported storage version")
      val unsupportedVersion = StorageVersions(0, 2, 0)
      mockedStore.storageVersion() returns Future.successful(Some(unsupportedVersion))

      When("migrate is called for that version")
      val ex = intercept[RuntimeException] {
        migrate.migrate()
      }

      Then("Migration exits with a readable error message")
      ex.getMessage should equal (s"Migration from versions < ${minVersion.str} are not supported. Your version: ${unsupportedVersion.str}")
    }

    "migrate throws an error for versions > current" in {
      val mockedStore = mock[PersistenceStore[_, _, _]]
      val migrate = migration(persistenceStore = mockedStore)

      Given("An unsupported storage version")
      val unsupportedVersion = StorageVersions(Int.MaxValue, Int.MaxValue, Int.MaxValue, StorageVersion.StorageFormat.PERSISTENCE_STORE)
      mockedStore.storageVersion() returns Future.successful(Some(unsupportedVersion))

      When("migrate is called for that version")
      val ex = intercept[RuntimeException] {
        migrate.migrate()
      }

      Then("Migration exits with a readable error message")
      ex.getMessage should equal (s"Migration from ${unsupportedVersion.str} is not supported as it is newer than ${StorageVersions.current.str}.")
    }

    "migrations are executed sequentially" in {
      val mockedStore = mock[PersistenceStore[_, _, _]]
      mockedStore.storageVersion() returns Future.successful(Some(StorageVersions(1, 4, 0, StorageVersion.StorageFormat.PERSISTENCE_STORE)))
      mockedStore.versions(any)(any) returns Source.empty
      mockedStore.ids()(any) returns Source.empty
      mockedStore.get(any)(any, any) returns Future.successful(None)
      mockedStore.getVersions(any)(any, any) returns Source.empty
      mockedStore.get(any, any)(any, any) returns Future.successful(None)
      mockedStore.store(any, any)(any, any) returns Future.successful(Done)
      mockedStore.store(any, any, any)(any, any) returns Future.successful(Done)
      mockedStore.setStorageVersion(any) returns Future.successful(Done)

      val migrate = migration(persistenceStore = mockedStore)
      migrate.appRepository.all() returns Source.empty
      migrate.groupRepository.rootVersions() returns Source.empty
      migrate.groupRepository.root() returns Future.successful(RootGroup.empty)
      migrate.groupRepository.storeRoot(any, any, any, any, any) returns Future.successful(Done)
      migrate.serviceDefinitionRepo.getVersions(any) returns Source.empty
      val result = migrate.migrate()
      result should be ('nonEmpty)
      result should contain theSameElementsInOrderAs migrate.migrations.map(_._1)
    }

    "log periodic messages if migration takes more time than usual" in {
      val mockedStore = mock[PersistenceStore[_, _, _]]
      val mockedLogger = mock[SLF4JLogger]

      mockedLogger.isInfoEnabled returns true
      val migrate = migration(
        persistenceStore = mockedStore,
        newLogger = Logger(mockedLogger),
        fakeMigrations = List(
          StorageVersions(1, 4, 2, StorageVersion.StorageFormat.PERSISTENCE_STORE) -> { () =>
            akka.pattern.after(1100.millis, system.scheduler) { //1100 because we want to catch 5 ticks of notifications
              Future.successful(Done)
            }
          }
        )
      )

      mockedStore.storageVersion() returns Future.successful(
        Some(StorageVersions(1, 4, 0, StorageVersion.StorageFormat.PERSISTENCE_STORE))
      )

      mockedStore.setStorageVersion(any) returns Future.successful(Done)

      migrate.migrate()

      verify(mockedLogger, atLeast(7)).info(any) //2 logging messages for init and shutdown, and 5 for waiting

      verify(mockedStore).storageVersion()
      verify(mockedStore).setStorageVersion(StorageVersions.current)
      noMoreInteractions(mockedStore)
    }
  }
}
