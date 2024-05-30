import {Execution, Loop, Token} from "../engine";
import {ServerComponent} from "../server";
import {IInstanceData} from "../interfaces";

export class InstanceMigrator extends ServerComponent {
  public constructor(server) {
    super(server)
    this.server = server
  }

  public migrate(prevState: IInstanceData, execution: Execution) {
    const oldTokens = prevState.tokens;

    const newTokens = this.migrateTokens(oldTokens, execution);
    // const loops = this.migrateLoops(prevState)

    newTokens.forEach((token) => execution.tokens.set(token.id, token));

  }

  private migrateTokens(tokens: Token[], targetExecution: Execution){
    const migratedTokens: Token[] = [];

    const findParentToken = (parentId: string) => {
      const foundParent = migratedTokens.find((token) => token.id === parentId);
      if (foundParent) {
        return foundParent;
      }

      const unprocessedParent = tokens.find((token) => token.id === parentId);
      if (!unprocessedParent) {
        throw new Error("Could not find parent token with id: " + parentId);
      }

      const token = this.processTokenMigration(unprocessedParent, targetExecution, findParentToken)
      migratedTokens.push(token);
      return token;
    }

    tokens.forEach((token) => {
      migratedTokens.push(this.processTokenMigration(token, targetExecution, findParentToken))
    })

    return migratedTokens;
  }

  private processTokenMigration(token: Token, targetExecution: Execution, findParentCallback: (parentId: string) => Token | undefined): Token {
    let migratedParentToken;
    if (token.parentToken) {
      migratedParentToken = findParentCallback(token.parentToken.id)

      if (!migratedParentToken) {
        const error = "***Error*** Node migration failed. Parent not found"
        this.logger.log(error)
        throw new Error(error)
      }
    }

    return this.migrateToken(token, targetExecution, migratedParentToken)
  }

  // TODO: Add migration of input and output fields
  // TODO: Add possibility to map the tokens to new nodes if theirs do not exist (creat a new Item then, and remove the current. Use "goNext")
  private migrateToken(token: Token, targetExecution: Execution, parent: Token): Token {
    const tokenCurrentNode = targetExecution.definition.getNodeById(token.currentNode?.id);
    const tokenStartNode = targetExecution.definition.getNodeById(token.currentNode?.id);

    if (!tokenCurrentNode || !tokenStartNode) {
      // TODO: Migrate with mapping in this case
      const error = "***Error*** Node migration failed"
      this.logger.log(error)
      throw new Error(error)
    }

    const migratedToken = new Token(token.type, targetExecution, tokenStartNode, token.dataPath, parent)
    migratedToken.id = token.id;
    migratedToken.startNodeId = token.startNodeId;
    migratedToken.currentNode = tokenCurrentNode;
    migratedToken.status = token.status;
    migratedToken.itemsKey = token.itemsKey;
    migratedToken.path = [];

    return migratedToken;
  }

  // TODO: Loop migration should have settings to restart looping or move it
  private migrateLoops(prevState: IInstanceData): Loop[] {
    if (!prevState.loops?.length) {
      this.logger.log("***Error*** Loop migration is not supported yet.")
    }

    return [];
  }

}