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

(*
type Node(x: int, y:int) as this =
    let id =x
    let other = y
    member this.GetId() = x

let a = [Node(4,5)]

for i in a do
    printfn "%d" (i.GetId())
*)

type MainCommands =
    | StartAlgorithm of (int*int)
    | Create of (int*IActorRef)
    | Notify of (int*IActorRef)
    | Stabilize
    | FindNewNodeSuccessor of (int*IActorRef)
    | FoundNewNodeSuccessor of (int*IActorRef)
    | PredecessorRequest
    | PredecessorResponse of (int*IActorRef)
    | KeyLookup of (int*int)
    | StartRequests
    | FixFingers
    | FindithSuccessor of (int*int*IActorRef)
    | FoundFingerEntry of (int*int*IActorRef)
    

let chordSystem = ActorSystem.Create("ChordSystem", configuration)
let mutable mainActorRef = null

let mutable numNodes = 0
let mutable numRequests = 0
let mutable m = 6
let mutable firstNodeRef = null
let mutable secondNodeRef = null
let StabilizeCycletimeMs = 50.0
let FixFingersCycletimeMs = 500.0

let mutable hashSpace = pown 2 m |> int

let updateElement index element list = 
  list |> List.mapi (fun i v -> if i = index then element else v)

//updateElement 4 40 [ 0 .. 9 ]


type FingerTableEntry(x:int, y:IActorRef) as this =
    let id = x
    let idRef = y
    member this.GetId() = x
    member this.GetRef() = y



let ChordNode (myId:int) (mailbox:Actor<_>) =    
   // let mutable hashSpace = pown(2, m) |> int
    let mutable firstNode = 0
    let mutable mySuccessor = 0
    let mutable mySuccessorRef = null
    let mutable myPredecessor = 0
    let mutable myPredecessorRef = null
    let mutable myFingerTable = []
    //let myFingerTable = new List<FingerTableEntry>()
    //myFingerTable.Capacity <- m
    let a = FingerTableEntry(0, null)
    let myFingerTable : FingerTableEntry[] = Array.create m a

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | Create (otherId, otherRef) ->
                // First two nodes in the Chord Ring
                mySuccessor <- otherId
                myPredecessor <- otherId
                mySuccessorRef <- otherRef
                myPredecessorRef <- otherRef
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)

            | Notify(predecessorId, predecessorRef) ->
                //if predecessorId > myPredecessor then
                myPredecessor <- predecessorId
                myPredecessorRef <- predecessorRef

            | FixFingers ->
                // from m-1 to 0 , calculate finger entry
                //printfn "\n %d Fix fingers" myId
                // FindithSuccessor(i, formulaId, mailbox.self, false)   <- call to self
                let mutable ithFinger = 0
                for i in 0..m-1 do
                    ithFinger <- myId + ( pown 2 i )
                    //printf "\n %d ka %d th finger = %d" myId i ithFinger
                    mailbox.Self <! FindithSuccessor(i, ithFinger, mailbox.Self)

            | FindithSuccessor(i, key, tellRef) ->
                //printfn "\n %d FindithFinger" myId
                if mySuccessor < myId && (key > myId || key < mySuccessor) then
                    tellRef <! FoundFingerEntry(i, mySuccessor, mySuccessorRef)
                    //printfn "\n %d (last node) Successor of %d is %d" myId key mySuccessor
                elif myId > key then 
                    myFingerTable.[m-1].GetRef() <! FindithSuccessor(i, key, tellRef)
                    //printfn "\n %d sending FindithSuccessor request to %d for key = %d" myId (myFingerTable.[m-1].GetId()) key
                elif key <= mySuccessor && key > myId then 
                    tellRef <! FoundFingerEntry(i, mySuccessor, mySuccessorRef)
                    //printfn "\n %d Successor of %d is %d" myId key mySuccessor
                else 
                    //mySuccessorRef <! FindithSuccessor(i, key, tellRef)
                    // CLOSEST PRECEDING NODE
                    for i = m-1 downto 0 do
                        let ithFinger = myFingerTable.[i].GetId()
                        //ithRef = myFingerTable.[i].GetRef()
                        if (ithFinger > myId && ithFinger <= key) then 
                            // find ref of actor myFingerTable[x] and send FindSuccessor to it
                            let ithRef = myFingerTable.[i].GetRef()
                            ithRef <! FindithSuccessor(i, key, tellRef)
                            //printfn "\n %d is telling %d to find successor for %d" myId ithFinger key

            | FoundFingerEntry(i, fingerId, fingerRef) ->
                // updated ith entry of my finger table
                //printfn "\n FoundFingerEntry"
                let tuple = FingerTableEntry(fingerId, fingerRef)
                myFingerTable.[i] <- tuple

            | Stabilize ->
                // Ask successor for its predecessor and wait
                mySuccessorRef <! PredecessorRequest

            | PredecessorResponse(predecessorOfSuccessor, itsRef) ->                    
                if predecessorOfSuccessor <> myId then
                    mySuccessor <- predecessorOfSuccessor
                    mySuccessorRef <- itsRef
                // Notify mysuccessor
                mySuccessorRef <! Notify(myId, mailbox.Self)
                
            | PredecessorRequest->    
                sender <! PredecessorResponse(myPredecessor, myPredecessorRef)

            | FoundNewNodeSuccessor(isId, isRef) ->
                // Update successor information of self
                mySuccessor <- isId
                mySuccessorRef <- isRef

                // populate fingertable entry with successor - it will get corrected in next FixFingers call
                for i in 0..m-1 do
                    let tuple = FingerTableEntry(isId, isRef)
                    myFingerTable.[i] <- tuple
                
                // start Stabilize scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                // start FixFingers scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)

                // Notify Successor
                mySuccessorRef <! Notify(myId, mailbox.Self)
         
            | KeyLookup(key, hopCount) ->
                (*if mySuccessor < myId && (key > myId || key < mySuccessor) then 
                    //seekerRef <! SuccessorOf(mySuccessor, mySuccessorRef)
                    printfn "\n %d key will be at %d node with hops = %d" key mySuccessor hopCount
                elif key <= mySuccessor && key > myId then 
                    // <! SuccessorOf(mySuccessor, mySuccessorRef)
                    printfn "\n %d key will be at %d node with hops = %d" key mySuccessor hopCount
                else 
                    mySuccessorRef <! KeyLookup(key, hopCount + 1)

                //
                *)
                if mySuccessor < myId && (key > myId || key < mySuccessor) then
                    printfn "\n %d key will be at %d node with hops = %d" key mySuccessor hopCount
                    //printfn "\n %d (last node) Successor of %d is %d" myId key mySuccessor
                elif myId > key then 
                    myFingerTable.[m-1].GetRef() <! KeyLookup(key, hopCount + 1)
                    //printfn "\n %d sending FindithSuccessor request to %d for key = %d" myId (myFingerTable.[m-1].GetId()) key
                elif key <= mySuccessor && key > myId then 
                    printfn "\n %d key will be at %d node with hops = %d" key mySuccessor hopCount
                    //printfn "\n %d Successor of %d is %d" myId key mySuccessor
                else 
                    //mySuccessorRef <! FindithSuccessor(i, key, tellRef)
                    // CLOSEST PRECEDING NODE
                    for i = m-1 downto 0 do
                        let ithFinger = myFingerTable.[i].GetId()
                        //ithRef = myFingerTable.[i].GetRef()
                        if (ithFinger > myId && ithFinger <= key) then 
                            // find ref of actor myFingerTable[x] and send FindSuccessor to it
                            let ithRef = myFingerTable.[i].GetRef()
                            ithRef <! KeyLookup(key, hopCount + 1)

            | FindNewNodeSuccessor(newId, seekerRef) ->
                if mySuccessor < myId && (newId > myId || newId < mySuccessor) then
                    seekerRef <! FoundNewNodeSuccessor(mySuccessor, mySuccessorRef)
                    //printfn "\n %d (last node) Successor of %d is %d" myId newId mySuccessor
                elif newId <= mySuccessor && newId > myId then 
                    seekerRef <! FoundNewNodeSuccessor(mySuccessor, mySuccessorRef)
                    //printfn "\n %d Successor of %d is %d" myId newId mySuccessor
                else 
                    mySuccessorRef <! FindNewNodeSuccessor(newId, seekerRef)
                    // CLOSEST PRECEDING NODE
                    (*for x in m .. 1 do
                        if (myFingerTable.[x] >=< (myId, newId)) then 
                            // find ref of actor myFingerTable[x] and send FindSuccessor to it
                            printfn "\n %d is telling %d to find successor for %d" myId myFingerTable.[x] newId*)

            | _ -> ()

            return! loop()
        }
    loop()

let MainActor (mailbox:Actor<_>) =    
    let mutable firstNodeId = 0
    let mutable secondNodeId = 0
    let mutable tempNodeId = 0
    let mutable tempNodeRef = null
    let mutable tempKey = 0

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | StartAlgorithm(numNodes, numRequests) ->
                let nodeArray = [|14;21;32;38;42;48;51;56|]

                firstNodeId <- 1
                printfn "\n\n ADDING %d" firstNodeId
                firstNodeRef <- spawn chordSystem (sprintf "%d" firstNodeId) (ChordNode firstNodeId)
                // Second Node
                secondNodeId <- 8
                printfn "\n\n ADDING %d" secondNodeId
                secondNodeRef <- spawn chordSystem (sprintf "%d" secondNodeId) (ChordNode secondNodeId)
                firstNodeRef <! Create(secondNodeId, secondNodeRef)
                secondNodeRef <! Create(firstNodeId, firstNodeRef)

                for x in 0..nodeArray.Length-1 do
                    tempNodeId <- nodeArray.[x] |> int
                    printfn "\n\n ADDING %d" tempNodeId
                    tempNodeRef <- spawn chordSystem (sprintf "%d" tempNodeId) (ChordNode tempNodeId)
                    firstNodeRef <! FindNewNodeSuccessor(tempNodeId, tempNodeRef)
                    System.Threading.Thread.Sleep(800)

                (*
                firstNodeId <- Random().Next(hashSpace)
                printfn "\n\n ADDING %d" firstNodeId
                firstNodeRef <- spawn chordSystem (sprintf "%d" firstNodeId) (ChordNode firstNodeId)
                // Second Node
                secondNodeId <- Random().Next(hashSpace)
                printfn "\n\n ADDING %d" secondNodeId
                secondNodeRef <- spawn chordSystem (sprintf "%d" secondNodeId) (ChordNode secondNodeId)
                firstNodeRef <! Create(secondNodeId, secondNodeRef)
                secondNodeRef <! Create(firstNodeId, firstNodeRef)

                for x in 3..numNodes do
                    System.Threading.Thread.Sleep(8000)
                    tempNodeId <- Random().Next(1, hashSpace)
                    printfn "\n\n ADDING %d" tempNodeId
                    tempNodeRef <- spawn chordSystem (sprintf "%d" tempNodeId) (ChordNode tempNodeId)
                    firstNodeRef <! FindNewNodeSuccessor(tempNodeId, tempNodeRef)
                *)
                printfn "\n Ring stabilized"
                System.Threading.Thread.Sleep(8000)
                mainActorRef <! StartRequests

            | StartRequests ->
                for x in 1..numRequests do
                    //tempKey <- Random().Next(1, hashSpace)
                    secondNodeRef <! KeyLookup(54, 1)
                    System.Threading.Thread.Sleep(8000)
                
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