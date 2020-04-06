;;;; thrift idl parser
;;;; https://thrift.apache.org/docs/idl

(in-package #:thrift)

(defvar *name-prefix* ""
  "When including a spec within a spec, prefix names with this string.")

(defun prefixize (s)
  (str *name-prefix* s))

(defun end-bracket-position (str)
  (position #\> str :from-end t))

(defun identifier-or-error (str)
  (if (every (lambda (c)
               (or (alphanumericp c)
                   (member c '(#\. #\_))))
             str)
      str
      (error "Not a valid thrift identifier: ~S." str)))

(defun parse-type (type-string)
  (cond
    ((find type-string #("bool" "byte" "i8" "i16" "i32" "i64" "double" "string" "binary" "slist") :test #'equal)
     (values (intern (string-upcase type-string) :keyword)))
    ((starts-with-p type-string "map<")
     (list :map (mapcar #'parse-type
                        (split (remove #\Space
                                       (subseq type-string
                                               #.(length "map<")
                                               (end-bracket-position type-string)))
                               #\,))))
    ((starts-with-p type-string "list<")
     (list :list (parse-type (subseq type-string #.(length "list<") (end-bracket-position type-string)))))
    ((starts-with-p type-string "set<")
     (list :set (parse-type (subseq type-string #.(length "set<") (end-bracket-position type-string)))))
    (t
     (identifier-or-error (prefixize type-string)))))

(defun tokenize-member (line)
  (let* ((no-default-line (subseq line 0 (position #\= line)))
         (container-type-open-bracket (position #\< no-default-line))
         (container-type-close-bracket (position #\> no-default-line :from-end t))
         (no-type-spaces-line (if container-type-open-bracket
                                  (remove #\Space no-default-line
                                          :start container-type-open-bracket
                                          :end container-type-close-bracket)
                                  no-default-line)))
    (remove "" (split no-type-spaces-line #\Space #\: #\;) :test #'equal)))

(defun parse-member (line)
  (let* ((tokens (tokenize-member line))
         (id (parse-integer (first tokens)))
         (requiredness (find (second tokens) '("required" "optional") :test #'equal))
         (type (parse-type (if requiredness (third tokens) (second tokens))))
         (name (identifier-or-error (if requiredness (fourth tokens) (third tokens)))))
    (list id type name)))

(defun parse-enum-member (line &optional implicit-value)
  (destructuring-bind (name value)
      (append (remove "" (split line #\Space #\, #\=) :test #'equal)
              (when implicit-value
                (list implicit-value)))
    (cons (identifier-or-error name) (if (integerp value)
                                         value
                                         (parse-integer value)))))

(defun parse-include-filename (line)
  (let ((open-quote (position #\" line))
        (end-quote (position #\" line :from-end t)))
    (subseq line (1+ open-quote) end-quote)))

(defun remove-block-comments (text)
  "Removes comments of the form /* ... */ from the supplied text."
  (with-output-to-string (s)
    (loop for current-position = 0 then (or end-comment (error "Unterminated block comment."))
       for start-comment = (search "/*" text :start2 current-position)
       for end-comment = (when start-comment
                           (let ((x (search "*/" text :start2 start-comment)))
                             (when x (+ x #.(length "*/")))))
       while start-comment do
         (write-string (subseq text current-position start-comment) s)
       finally
         (write-string (subseq text current-position) s))))

(defun handle-inline-comments (line)
  "Removes inline comments for # and //"
  (let* ((line-end (length line))
         (comment-pos (or (position #\# line) line-end))
         (c-style-comment-pos (or (search "//" line) line-end))
         (comments-removed (subseq line 0 (min comment-pos c-style-comment-pos))))    
    (when (notevery (lambda (c) (char= c #\Space))
                    comments-removed)
      comments-removed)))

(defun parse-spec (filename &optional included-p)
  "Parses the thrift spec located at FILENAME into a list."
  (let ((thrift-lines (split (remove-block-comments (file-to-string filename)) #\Newline))
        (*name-prefix* (if included-p
                           (str (pathname-name filename) ".")
                           ""))
        struct
        enum
        enum-index
        results)
    (dolist (raw-line thrift-lines results)
      (let ((line (handle-inline-comments raw-line)))
        (when line
          (cond
            ((starts-with-p line "include")
             (setf results (append (parse-spec (merge-pathnames (parse-include-filename line) filename) t)
                                   results)))
            ((starts-with-p line "struct")
             (push (identifier-or-error (prefixize (string-trim " {" (subseq line (length "struct ")))))
                   struct))
            ;; treat union as a struct, since "a union is encoded exactly the same as a struct with the
            ;; additional restriction that at most 1 field may be encoded."
            ((starts-with-p line "union")
             (push (identifier-or-error (prefixize (string-trim " {" (subseq line (length "union ")))))
                   struct))
            ((starts-with-p line "enum")
             (setf enum-index 0)
             (push (identifier-or-error (prefixize (string-trim " {" (subseq line (length "enum ")))))
                   enum))
            ((and struct (find #\} line))
             (push (reverse struct) results)
             (setf struct nil))
            ((and enum (find #\} line))
             (push (reverse enum) results)
             (setf enum nil))
            ((and struct (find #\: line))
             (push (parse-member line) struct))
            ((and enum (find #\= line))
             (let* ((parsed-enum (parse-enum-member line))
                    (enum-value (cdr parsed-enum)))
               (if (< enum-value enum-index)
                   (error "Enum values should be strictly increasing.")
                   (setf enum-index (1+ enum-value)))
               (push parsed-enum enum)))
            (enum
             (push (parse-enum-member line enum-index) enum)
             (incf enum-index))))))))
