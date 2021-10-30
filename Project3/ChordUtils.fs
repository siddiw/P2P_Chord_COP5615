module ChordNode

open System
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Collections.Generic

type MainCommands =
    | StartAlgorithm of (int*int)