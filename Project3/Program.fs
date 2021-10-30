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

let inline (>=<) a (b,c) = a >= b && a<= c

type MainCommands =
    | StartAlgorithm of (int*int)
    | Create of (int*IActorRef)
    | Notify of (int*IActorRef)
    | Stabilize
    | FindSuccessor of (int*IActorRef)
    | TellYourPredecessor
    | Join2 of (int*int)
    | TellingPredecessor of (int*IActorRef)
    | RequestingPredecessor
    | YourSuccessor of (int*IActorRef)

let chordSystem = ActorSystem.Create("ChordSystem", configuration)
let mutable mainActorRef = null

let mutable numNodes = 0
let mutable numRequests = 0
let mutable m = 160
let mutable firstNodeRef = null
let mutable secondNodeRef = null
let StabilizeCycletimeMs = 50.0

let mutable hashSpace = pown 2 m |> int


type fingerTable_entry =
    struct
        val chordId: int64
    end

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
            | Notify(predecessorId, predecessorRef) ->
                //if predecessorId > myPredecessor then
                    myPredecessor <- predecessorId
                    myPredecessorRef <- predecessorRef
            | Stabilize ->
                printfn "\n Stabilize"
                // Ask successor for its predecessor and wait
                mySuccessorRef <! RequestingPredecessor
            | TellingPredecessor(predecessorOfSuccessor, itsRef) ->
                if predecessorOfSuccessor <> myId then
                     mySuccessor <- predecessorOfSuccessor
                     mySuccessorRef <- itsRef
                // Notify mysuccessor
                mySuccessorRef <! Notify(myId, mailbox.Self)
            | RequestingPredecessor->    
                // Find my successor ref and send Notify
                sender <! TellingPredecessor(myPredecessor, myPredecessorRef)
            | FindSuccessor(newId, newRef) ->
                if newId >=< (myId, mySuccessor) then 
                    // tell id that this is your successor
                    // find reference of id and then send SuccessorFound() message to it
                    newRef <! YourSuccessor(myId, mailbox.Self)
                else 
                    // CLOSEST PRECEDING NODE
                    for x in m .. 1 do
                        if (myFingerTable.[x] >=< (myId, newId)) then 
                            // find ref of actor myFingerTable[x] and send FindSuccessor to it
                            printfn "\n %d is telling %d to find successor for %d" myId myFingerTable.[x] newId

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

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | StartAlgorithm(numNodes, numRequests) ->
                firstNodeId <- Random().Next(hashSpace)
                firstNodeRef <- spawn chordSystem (sprintf "%d" firstNodeId) (ChordNode firstNodeId)
                // Second Node
                secondNodeId <- Random().Next(hashSpace)
                secondNodeRef <- spawn chordSystem (sprintf "%d" secondNodeId) (ChordNode secondNodeId)
                firstNodeRef <! Create(secondNodeId, secondNodeRef)
                secondNodeRef <! Create(firstNodeId, firstNodeRef)


            

            | _ -> ()

            return! loop()
        }
    loop()




[<EntryPoint>]
let main argv =
    if (argv.Length <> 2) then printfn "Starting with default values" 
    else 
        numNodes <-  argv.[0] |> int
        numRequests <- argv.[1] |> int

    mainActorRef <- spawn chordSystem "MainActor" MainActor
    mainActorRef <! StartAlgorithm(numNodes, numRequests)
        
    0