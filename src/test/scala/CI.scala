/**
  * Created by xiaonuo on 8/3/16.
  */
object CI extends App {

  case class User()

  trait Repository {
    def save(user: User)
  }

  trait DefaultRepository extends Repository {
    def save(user: User) = {
      println("save user")
    }
  }

  trait UserService extends Repository{
//    self: Repository =>
    def create(user: User): Unit = {
      save(user)
    }
  }

  (new UserService with DefaultRepository).create(new User)

}
