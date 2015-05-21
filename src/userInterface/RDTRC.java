package rdtrc.userInterface;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import rdtrc.fileManagement.FileManagement;

public class RDTRC {

	public static void main(String[] args) {

		FileManagement fileManager = FileManagement.getInstance();
		fileManager.populateConfigurationItems();
		
		System.out.println("Welcome to RDTRC.");
		System.out
				.println("You can specify a script file (.rdtrc) for processing or a folder containing several script files (.rdtrc) for bulk execution.");
		System.out
				.println("To exit from the environment at any point, please enter QUIT.");

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input = null;

		ScriptExecutor executor = ScriptExecutor.getInstance();

		do {
			System.out
					.print("Enter script path to process or type QUIT to exit: ");
			try {
				input = br.readLine();
			} catch (IOException ioe) {
				System.exit(1);
			}

			if (input.equalsIgnoreCase("QUIT")) {
				System.out.println("Thank you for using RDTRC.");
				System.exit(0);
			}

			executor.processUserInput(input);
			executor.flushStructuresToDisk();
		} while (true);

	}

}
