package org.coderic.accounting.endpoint;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.RetryPolicy;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class EndpointApplication {
	public static IonSystem ionSys = IonSystemBuilder.standard().build();
	public static QldbDriver qldbDriver;
	public static void main(String[] args) {
		SpringApplication.run(EndpointApplication.class, args);
		System.out.println("Initializing the driver");
		qldbDriver = QldbDriver.builder()
				.ledger("quick-start")
				.transactionRetryPolicy(RetryPolicy
						.builder()
						.maxRetries(3)
						.build())
				.sessionClientBuilder(QldbSessionClient.builder())
				.build();
		qldbDriver.execute(txn -> {
			System.out.println("Creating a table and an index");
			txn.execute("CREATE TABLE People");
			txn.execute("CREATE INDEX ON People(lastName)");
		});
		qldbDriver.execute(txn -> {
			System.out.println("Inserting a document");
			IonStruct person = ionSys.newEmptyStruct();
			person.put("firstName").newString("John");
			person.put("lastName").newString("Doe");
			person.put("age").newInt(32);
			txn.execute("INSERT INTO People ?", person);

		});
		qldbDriver.execute(txn -> {
			System.out.println("Querying the table");
			Result result = txn.execute("SELECT * FROM People WHERE lastName = ?",
					ionSys.newString("Doe"));
			IonStruct person = (IonStruct) result.iterator().next();
			System.out.println(person.get("firstName")); // prints John
			System.out.println(person.get("lastName")); // prints Doe
			System.out.println(person.get("age")); // prints 32
		});
		qldbDriver.execute(txn -> {
			System.out.println("Updating the document");
			final List<IonValue> parameters = new ArrayList<>();
			parameters.add(ionSys.newInt(42));
			parameters.add(ionSys.newString("Doe"));
			txn.execute("UPDATE People SET age = ? WHERE lastName = ?", parameters);
		});
		qldbDriver.execute(txn -> {
			System.out.println("Querying the table for the updated document");
			Result result = txn.execute("SELECT * FROM People WHERE lastName = ?",
					ionSys.newString("Doe"));
			IonStruct person = (IonStruct) result.iterator().next();
			System.out.println(person.get("firstName")); // prints John
			System.out.println(person.get("lastName")); // prints Doe
			System.out.println(person.get("age")); // prints 32
		});
	}

}
