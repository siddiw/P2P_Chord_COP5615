open System
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Collections.Generic
open ChordNode

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : ERROR
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

type Node(x: int, y:int) as this =
    let id =x
    let other = y
    member this.GetId() = x
    


let a = [Node(4,5)]

for i in a do
    printfn "%d" (i.GetId())

type MainCommands =
    | StartAlgorithm of (int*int)
    | Create of (int*IActorRef)
    | Notify of (int*IActorRef)
    | Stabilize
    | FindSuccessor of (int*IActorRef)
    | TellYourPredecessor
    | Join2 of (int*int)
    | PredecessorResponse of (int*IActorRef)
    | PredecessorRequest
    | YourSuccessor of (int*IActorRef)

let chordSystem = ActorSystem.Create("ChordSystem", configuration)
let mutable mainActorRef = null

let mutable numNodes = 0
let mutable numRequests = 0
let mutable m = 6
let mutable firstNodeRef = null
let mutable secondNodeRef = null
let StabilizeCycletimeMs = 5000000.0

let mutable hashSpace = pown 2 m |> int

type FingerTableEntry(x:int, y:IActorRef) as this =
    let id = x
    let idRef = y
    member this.GetId() = x

let stabilize ()=
    // called periodically
    printfn "\n Stabilize"

let fix_fingers ()=
    // called periodically
    printfn "\n Fix Fingers"

let check_predecessor ()=
    // called periodically
    printfn "\n Check Predecessor"


let Node (id:int) (mailbox:Actor<_>) =
    let mutable myNeighboursArray = []
    let mutable myRumourCount = 0
    let myActorIndex = id
    let mutable isActive = 1

    printfn "\n Node"
 

let ChordNode (myId:int) (mailbox:Actor<_>) =    
   // let mutable hashSpace = pown(2, m) |> int
    let mutable firstNode = 0
    let mutable mySuccessor = 0
    let mutable mySuccessorRef = null
    let mutable myPredecessor = 0
    let mutable myPredecessorRef = null
    let mutable myFingerTable = []

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | StartAlgorithm(numNodes, numRequests) ->
                firstNode <- Random().Next(m)
            | Create (otherId, otherRef) ->
                // First two nodes in the Chord Ring
                mySuccessor <- otherId
                myPredecessor <- otherId
                mySuccessorRef <- otherRef
                myPredecessorRef <- otherRef
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                printfn "\n %d added %d as predecessor and %d as successor" myId myPredecessor mySuccessor
            | Notify(predecessorId, predecessorRef) ->
                //if predecessorId > myPredecessor then
                    myPredecessor <- predecessorId
                    myPredecessorRef <- predecessorRef
            | Stabilize ->
                printfn "\n %d Stabilize" myId
                // Ask successor for its predecessor and wait
                mySuccessorRef <! PredecessorRequest

            | PredecessorResponse(predecessorOfSuccessor, itsRef) ->    
                printfn "\n PredecessorResponse of %d = %d" myId predecessorOfSuccessor
                
                if predecessorOfSuccessor <> myId then
                     mySuccessor <- predecessorOfSuccessor
                     mySuccessorRef <- itsRef
                printfn "\n predecessor of %d = %d" myId myPredecessor
                printfn "\n successor of %d = %d" myId mySuccessor
                // Notify mysuccessor
                mySuccessorRef <! Notify(myId, mailbox.Self)
            | PredecessorRequest->    
                // Find my successor ref and send Notify
                printfn "\n %d sending Precedeccsor Response to %s" myId sender.Path.Name
                sender <! PredecessorResponse(myPredecessor, myPredecessorRef)
            | YourSuccessor(id, ref) ->
                mySuccessor <- id
                mySuccessorRef <- ref
                // initially input successor id and ref in all m places of finger table 
                for x in 0..m do
                    //myFingerTable <- myFingerTable :: [mySuccessor;mySuccessorRef]
                    let a = [FingerTableEntry(id, ref)]
                    myFingerTable <- a :: myFingerTable
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                printfn "\n %d found successor = %d" myId mySuccessor

            //| Join (id, ref) ->
            //    mailbox.Self <! FindSuccessor(id, ref)
            | FindSuccessor(newId, newRef) ->
                printfn "\n Finding successor for %d" newId

                
                    
                if newId >= myId then 
                    if newId <= mySuccessor then 
                        // tell id that this is your successor
                        // find reference of id and then send SuccessorFound() message to it
                        newRef <! YourSuccessor(mySuccessor, mySuccessorRef)
                        printfn "\n Now sending Found successor for %d" newId
                    else 
                        mySuccessorRef <! FindSuccessor(newId, newRef)
                        // CLOSEST PRECEDING NODE
                        (*for x in m .. 1 do
                            if (myFingerTable.[x] >=< (myId, newId)) then 
                                // find ref of actor myFingerTable[x] and send FindSuccessor to it
                                printfn "\n %d is telling %d to find successor for %d" myId myFingerTable.[x] newId*)
                if mySuccessor < myId && newId > myId then 
                    newRef <! YourSuccessor(mySuccessor, mySuccessorRef)
                    printfn "\n Successor of %d is %d" newId mySuccessor

                (*if newId < myId then
                    if newId > myPredecessor then 
                        newRef <! YourSuccessor(myId, mailbox.Self)
                    else
                        myPredecessorRef <! FindSuccessor(newId, newRef) *)

            | _ -> ()

            return! loop()
        }
    loop()


let MainActor (mailbox:Actor<_>) =    
    let mutable actorsDone = 0
    let mutable actorsThatKnow = 0
    let mutable topologyBuilt = 0
    let mutable firstNodeId = 0
    let mutable secondNodeId = 0
    let mutable tempNodeId = 0
    let mutable tempNodeRef = null


    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | StartAlgorithm(numNodes, numRequests) ->
                //firstNodeId <- Random().Next(hashSpace)
                firstNodeId <- 1
                firstNodeRef <- spawn chordSystem (sprintf "%d" firstNodeId) (ChordNode firstNodeId)
                // Second Node
                //secondNodeId <- Random().Next(hashSpace)
                secondNodeId <- 8
                secondNodeRef <- spawn chordSystem (sprintf "%d" secondNodeId) (ChordNode secondNodeId)
                firstNodeRef <! Create(secondNodeId, secondNodeRef)
                secondNodeRef <! Create(firstNodeId, firstNodeRef)

                // add remaining nodes
                tempNodeId <- 14
                tempNodeRef <- spawn chordSystem (sprintf "%d" tempNodeId) (ChordNode tempNodeId)
                firstNodeRef <! FindSuccessor(tempNodeId, tempNodeRef)


                tempNodeId <- 21
                tempNodeRef <- spawn chordSystem (sprintf "%d" tempNodeId) (ChordNode tempNodeId)
                firstNodeRef <! FindSuccessor(tempNodeId, tempNodeRef)
                

            | _ -> ()

            return! loop()
        }
    loop()




[<EntryPoint>]
let main argv =
    numNodes <-  argv.[0] |> int
    numRequests <- argv.[1] |> int

    mainActorRef <- spawn chordSystem "MainActor" MainActor
    mainActorRef <! StartAlgorithm(numNodes, numRequests)

    chordSystem.WhenTerminated.Wait()
        
    0