;;;; thrift binary (TBinaryProtocol) decoder
;;;; http://slackhappy.github.io/thriftfiddle/tbinaryspec.html
;;;; also https://erikvanoosten.github.io/thrift-missing-specification/

(in-package #:thrift)

;;; utilities

(defstruct buf
  (bytes nil :type (simple-array (unsigned-byte 8)))
  (pos 0 :type fixnum))

(defun read-buf (buf count)
  "Read COUNT number of bytes, returning them as a single integer."
  (declare (fixnum count))
  (let* ((pos (buf-pos buf))
         (new-pos (+ pos count))
         (shift -8))
    (setf (buf-pos buf) new-pos)
    (reduce #'logior (buf-bytes buf)
            :start pos
            :end new-pos
            :key (lambda (x)
                   (declare ((unsigned-byte 8) x))
                   (ash x (incf shift 8)))
            :from-end t)))

(defun file-to-vector (filename)
  (with-open-file (f filename :element-type 'unsigned-byte)
    (let* ((len (file-length f))
           (contents (make-array len :element-type '(unsigned-byte 8))))
      (read-sequence contents f)
      contents)))

;;; decoder

(defvar *thrift-spec*)

(defun type-name (type-tag)
  (elt #(:stop :void :bool :byte :double nil :i16 nil :i32
         nil :i64 :string :struct :map :set :list :enum) type-tag))

(defun get-struct-def (name)
  (rest (assoc name *thrift-spec* :test #'equal)))

(defun struct-field-name (struct-def id)
  (third (assoc id struct-def)))

(defun struct-field-type (struct-def id)
  (second (assoc id struct-def)))

(defun read-type (buf)
  (type-name (read-buf buf 1)))

(defun read-id (buf)
  (read-buf buf 2))

(defgeneric read-value (type buf &optional struct-name)
  (:method ((type (eql :struct)) buf &optional struct-name)
    (let* ((struct-def (get-struct-def struct-name))
           ;; allocate the result map. structs may have as little as one or two keys, so we supply a
           ;; hash table size to save memory.
           (struct-map (make-hash-table :test 'equal :size (length struct-def))))
      (loop for member-type = (read-type buf)
         until (eql member-type :stop)
         do (let* ((member-id (read-id buf))
                   (member-name (struct-field-name struct-def member-id))
                   (member-value (read-value member-type buf
                                             (case member-type
                                               (:struct
                                                (struct-field-type struct-def member-id))
                                               ((:list :set :map)
                                                ;; (:list TYPE)
                                                ;; (:set TYPE)
                                                ;; (:map (KEY-TYPE VALUE-TYPE))
                                                (second (struct-field-type struct-def member-id)))))))
              (setf (gethash member-name struct-map) member-value)))
      struct-map))
  (:method ((type (eql :bool)) buf &optional struct-name)
    (declare (ignore struct-name))
    (if (zerop (read-buf buf 1)) 'yason:false 'yason:true))
  (:method ((type (eql :double)) buf &optional struct-name)
    (declare (ignore struct-name))
    (ieee-floats:decode-float64 (read-buf buf 8)))
  (:method ((type (eql :i16)) buf &optional struct-name)
    (declare (ignore struct-name))
    (read-buf buf 2))
  (:method ((type (eql :i32)) buf &optional struct-name)
    (declare (ignore struct-name))
    (read-buf buf 4))
  (:method ((type (eql :i64)) buf &optional struct-name)
    (declare (ignore struct-name))
    (read-buf buf 8))
  (:method ((type (eql :string)) buf &optional struct-name)
    (declare (ignore struct-name))
    (let ((len (read-buf buf 4)))
      (prog1 (trivial-utf-8:utf-8-bytes-to-string (buf-bytes buf)
                                                  :start (buf-pos buf)
                                                  :end (+ (buf-pos buf) len))
        (incf (buf-pos buf) len))))
  (:method ((type (eql :list)) buf &optional struct-name)
    (let* ((element-type (read-type buf))
           (len (read-buf buf 4)))
      (loop for i from 1 to len collect (read-value element-type buf
                                                    ;; if the element type is itself a container,
                                                    ;; struct-name will be a list, so obtain the
                                                    ;; element type for the read-value call
                                                    ;;
                                                    ;; element is value type, NIL => NIL
                                                    ;; element is struct, TYPE-NAME => TYPE-NAME
                                                    ;; element is list, (:list TYPE) => TYPE
                                                    ;; element is set, (:set TYPE) => TYPE
                                                    ;; element is map, (:map (KEY-TYPE VALUE-TYPE)) => (KEY-TYPE VALUE-TYPE)
                                                    (if (consp struct-name)
                                                        (cadr struct-name)
                                                        struct-name)))))
  (:method ((type (eql :set)) buf &optional struct-name)
    (let* ((element-type (read-type buf))
           (len (read-buf buf 4)))
      (loop for i from 1 to len collect (read-value element-type buf
                                                    (if (consp struct-name)
                                                        (cadr struct-name)
                                                        struct-name)))))
  (:method ((type (eql :map)) buf &optional struct-name-pair)
    (let* ((key-type (read-type buf))
           (value-type (read-type buf))
           (key-struct-name (first struct-name-pair))
           (value-struct-name (second struct-name-pair))
           (len (read-buf buf 4))
           (result-hash-table (make-hash-table :test 'equal :size len)))
      (dotimes (i len result-hash-table)
        (setf (gethash (read-value key-type buf key-struct-name) result-hash-table)
              (read-value value-type buf value-struct-name)))))
  (:method (type buf &optional struct-name)
    (declare (ignore struct-name))
    (error "Unsupported thrift type: ~S." type)))

(defun thriftjson (idl-file bin-file)
  (let ((s (yason:make-json-output-stream *standard-output*))
        (*thrift-spec* (parse-spec idl-file))
        (buf (make-buf :bytes (file-to-vector bin-file))))
    (yason:encode (read-value :struct buf "Config") s)))

(defun decode-struct (thrift-spec byte-array struct-type-name)
  "Decodes a thrift struct of STRUCT-TYPE-NAME from BYTE-ARRAY according to THRIFT-SPEC. BYTE-ARRAY
is assumed to be the serialized struct, encoded according to TBinaryProtocol, without any other
leading or trailing data. Returns a hash table with string keys, and values converted to the
corresponding lisp type."
  (check-type thrift-spec cons "a parsed thrift spec as returned by PARSE-SPEC")
  (check-type byte-array (simple-array (unsigned-byte 8)))
  (check-type struct-type-name string "a string naming a struct type defined in THRIFT-SPEC")
  (let ((*thrift-spec* thrift-spec)
        (buf (make-buf :bytes byte-array)))
    (unless (get-struct-def struct-type-name)
      (error "Could not find a struct named ~S in the provided thrift spec." struct-type-name))
    (read-value :struct buf struct-type-name)))
