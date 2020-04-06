;;;; thrift encoder and decoder
;;;; used by jsonthrift and thriftjson scripts in mobile/scripts

(require "wu-sugar")
(require "yason")
(require "ieee-floats")
(require "trivial-utf-8")

(defpackage #:thrift
  (:use #:cl #:wu-sugar)
  (:export
   #:jsonthrift
   #:thriftjson
   #:parse-spec
   #:decode-struct))

(load "thrift/idl")
(load "thrift/decode")
(load "thrift/encode")
