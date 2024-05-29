import {Definition} from "../elements";
import {Loop, Token} from "../engine";
import {ServerComponent} from "../server";
import {IExecution, IInstanceData} from "../interfaces";

export class InstanceMigrator extends ServerComponent {
  public constructor(server) {
    super(server)
    this.server = server
  }

  public migrate(prevState: IInstanceData, execution: IExecution) {
    const oldTokens = prevState.tokens;

    const migratedTokens = oldTokens.map((token: Token) => this.migrateToken(token, prevState))
  }

  // TODO: Add migration of input and output fields
  // TODO: Add possibility to map the tokens to new nodes if theirs do not exist (creat a new Item then, and remove the current. Use "goNext")
  private migrateToken(token: Token, relatedState: IInstanceData, targetExecution: IExecution) {
    const tokenNode = targetExecution.definition.getNodeById(token.currentNode?.id);
    if (!tokenNode) {
      this.logger.log("***Error*** Node migration failed")
    }

    targetExecution.tokens.set()

    return true;
  }

  // TODO: Loop migration should have settings to restart looping or move it
  private validateLoop(loop: Loop, definition: Definition) {
    const loopNode = definition.getNodeById(loop.node.id);
    if (!loopNode) {
      this.logger.log("***Error*** Migration failed")
    }

    return true;
  }

}