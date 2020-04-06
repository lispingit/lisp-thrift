;;;; thrift binary encoder
;;;; http://slackhappy.github.io/thriftfiddle/tbinaryspec.html
;;;; also https://erikvanoosten.github.io/thrift-missing-specification/

(in-package #:thrift)

(defun json-to-plist (json-file)
  (let ((*read-default-float-format* 'double-float))
    (yason:parse (file-to-string json-file) :object-as :plist :json-booleans-as-symbols t)))

(defun enum-def-p (def)
  (every #'integerp (mapcar #'cdr def)))

(defconstant +bool-tag+ #x02)
(defconstant +byte-tag+ #x03)
(defconstant +double-tag+ #x04)
(defconstant +i16-tag+ #x06)
(defconstant +i32-tag+ #x08)
(defconstant +i64-tag+ #x0A)
(defconstant +string-tag+ #x0B)
(defconstant +struct-tag+ #x0C)
(defconstant +map-tag+ #x0D)
(defconstant +set-tag+ #x0E)
(defconstant +list-tag+ #x0F)

(defun struct-field-encoding (struct-def field-name)
  (let ((idl-field-def (find field-name struct-def :key #'third :test #'equal))
        (base-types `(:bool ,+bool-tag+ :byte ,+byte-tag+ :double ,+double-tag+ :i16 ,+i16-tag+
                            :i32 ,+i32-tag+ :i64 ,+i64-tag+ :string ,+string-tag+
                            :map ,+map-tag+ :set ,+set-tag+ :list ,+list-tag+)))
    (destructuring-bind (id type idl-field-name) idl-field-def
      (declare (ignore idl-field-name))
      (let ((type-tag (cond
                        ((and (keywordp type) (member type base-types))
                         (getf base-types type))
                        ((stringp type)
                         (if (enum-def-p (get-struct-def type))
                             +i32-tag+
                             +struct-tag+))
                        (t
                         (error "Unsupported field type: ~S." type)))))
        (values type-tag id type)))))

(defun write-int (int byte-count stream)
  (let (bytes)
    (dotimes (i byte-count)
      (push (logand int #xFF) bytes)
      (setf int (ash int -8)))
    (mapc (lambda (b) (write-byte b stream)) bytes)))

(declaim (ftype (function) write-struct-value))

(defgeneric write-value (type-tag id value stream &optional struct-name)
  (:method :before (type-tag id value stream &optional struct-name)
           (declare (ignore struct-name))
           (write-byte type-tag stream)
           (write-byte (ldb (byte 8 8) id) stream)
           (write-byte (ldb (byte 8 0) id) stream))
  (:method ((type-tag (eql +struct-tag+)) id value stream &optional struct-name)
    (declare (ignore type-tag id))
    (write-struct-value value struct-name stream))
  (:method ((type-tag (eql +bool-tag+)) id value stream &optional struct-name)
    (declare (ignore struct-name))
    (write-byte (ecase value (yason:true 1) (yason:false 0)) stream))
  (:method ((type-tag (eql +byte-tag+)) id value stream &optional struct-name)
    (declare (ignore struct-name))
    (write-byte value stream))
  (:method ((type-tag (eql +double-tag+)) id value stream &optional struct-name)
    (declare (ignore struct-name))
    (write-int (ieee-floats:encode-float64 value) 8 stream))
  (:method ((type-tag (eql +i16-tag+)) id value stream &optional struct-name)
    (declare (ignore struct-name))
    (write-int value 2 stream))
  (:method ((type-tag (eql +i32-tag+)) id value stream &optional struct-name)
    (declare (ignore struct-name))
    (write-int value 4 stream))
  (:method ((type-tag (eql +i64-tag+)) id value stream &optional struct-name)
    (declare (ignore struct-name))
    (write-int value 8 stream))
  (:method ((type-tag (eql +string-tag+)) id value stream &optional struct-name)
    (declare (ignore struct-name))
    (let ((len (length value)))
      (write-int len 4 stream)
      (trivial-utf-8:write-utf-8-bytes value stream)))
  (:method (type-tag id value stream &optional struct-name)
    (declare (ignore id value stream struct-name))
    (error "Unsupported thrift type tag: ~S." type-tag)))

(defun write-struct-value (value struct-name stream)
  (let ((struct-def (get-struct-def struct-name)))
    (loop for (field-name member-val) on value by #'cddr do
         (multiple-value-bind (type-tag id type-name)
             (struct-field-encoding struct-def field-name)
           (write-value type-tag id member-val stream type-name)))
    (write-byte 0 stream)))

(defun plist-to-bin (idl-file struct-name plist out-file)
  (let* ((*thrift-spec* (parse-spec idl-file)))
    (with-open-file (f out-file :direction :output :element-type 'unsigned-byte :if-exists :supersede)
      (write-struct-value plist struct-name f))))

(defun jsonthrift (idl-file struct-name json-file out-file)
  (plist-to-bin idl-file struct-name (json-to-plist json-file) out-file))
