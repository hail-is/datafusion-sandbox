# Separate input and output formats

Input and output formats are separate types even though they currently support the same formats.
VCF is expected to become a readable input without becoming a supported output, and separate types
will make that asymmetry a compile-time constraint instead of a runtime validation step.
