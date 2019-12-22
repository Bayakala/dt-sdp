package protobuf.bytes
import java.io.{ByteArrayInputStream,ByteArrayOutputStream,ObjectInputStream,ObjectOutputStream}
import com.google.protobuf.ByteString
object Converter {

  def marshal(value: Any): ByteString = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    ByteString.copyFrom(stream.toByteArray())
  }

  def unmarshal[A](bytes: ByteString): A = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    val value = ois.readObject()
    ois.close()
    value.asInstanceOf[A]
  }


}