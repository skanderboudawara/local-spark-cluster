// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from "vscode";
import { exec } from "child_process";
import * as fs from "fs";
// This methotsc --watchd is called when your extension is activated
// Your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {
  // The commandId parameter must match the command field in package.json
  const disposable = vscode.commands.registerCommand(
    "spark-job-submit.submitjob",
    async () => {
      // The code you place here will be executed every time your command is executed
      // Display a message box to the user
      // vscode.window.showInformationMessage('Hello World from spark job submit!');
      const editor = vscode.window.activeTextEditor;
      if (!editor) {
        vscode.window.showErrorMessage("No active file found.");
        return;
      }

      const filePath = editor.document.fileName;

      // Get the relative path based on the workspace folder
      const relativePath = vscode.workspace.asRelativePath(filePath);
      console.log(relativePath);

      // Get the Docker cluster name from settings
      let dockerClusterName = vscode.workspace
        .getConfiguration()
        .get<string>("sparkJobSubmit.dockerClusterName");

      if (!dockerClusterName) {
        // If not found in settings, ask the user for the Docker cluster name
        const input = await vscode.window.showInputBox({
          prompt: "Enter the name of the Docker cluster",
          placeHolder: "spark-cluster-spark-master-1",
        });

        if (input) {
          dockerClusterName = input;

          // Save the entered Docker cluster name to settings
          await vscode.workspace
            .getConfiguration()
            .update(
              "sparkJobSubmit.dockerClusterName",
              dockerClusterName,
              vscode.ConfigurationTarget.Global
            );
        }
      }

      if (!dockerClusterName) {
        vscode.window.showErrorMessage("Docker cluster name is required.");
        return;
      }

      console.log(`Using Docker cluster: ${dockerClusterName}`);

      // Prepare the docker exec command
      const command = `docker exec -it ${dockerClusterName} spark-submit //${relativePath}`;
      console.log(command);

      // Read the content of the current file
      fs.readFile(filePath, "utf8", (err, data) => {
        if (err) {
          vscode.window.showErrorMessage("Error reading file.");
          return;
        }

        // Check if the file contains @compute
        if (data.includes("spark") || data.includes("compute")) {
          const existingTerminal = vscode.window.terminals.find(
            (term) => term.name === "Spark Submit"
          );

          if (existingTerminal) {
            // If the terminal exists, reuse it and send the command
            existingTerminal.sendText(command);
            existingTerminal.show(); // Focus the terminal
          } else {
            // If no terminal exists, create a new one
            const terminal = vscode.window.createTerminal({
              name: "Spark Submit",
              cwd: vscode.workspace.workspaceFolders
                ? vscode.workspace.workspaceFolders[0].uri.fsPath
                : undefined,
            });

            // Write and execute the Docker command in the terminal
            terminal.sendText(command);
            terminal.show(); // Focus the terminal
          }
        } else {
          vscode.window.showErrorMessage("There is no @compute in this file.");
        }
      });
    }
  );

  context.subscriptions.push(disposable);
}

// This method is called when your extension is deactivated
export function deactivate() {}
