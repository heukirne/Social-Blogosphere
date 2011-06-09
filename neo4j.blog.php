<?php

require('php-neo-rest.php');

class IndexNode extends Node
{
	var $_idx_srv;
	var $_in_index;
	var $_index_property;
	
	public function __construct($neo_db, $index_service, $index_property)
	{
		parent::__construct($neo_db);
		$this->_idx_srv = $index_service;
		$this->_index_property = $index_property;
		$this->_in_index = FALSE;
	}
	
	public function save() {	
		if ($this->_is_new)
		{
			try {
				$node = $this->_idx_srv->getNode($this->_index_property, str_replace(' ','%20',$this->{$this->_index_property}));
				$this->_id = $node->getId();
				$this->_is_new = FALSE;
				$this->_in_index = TRUE;
			}
			catch (NotFoundException $e) { }
		}
		parent::save();
		if (!$this->_in_index) {
			$this->_idx_srv->index($this, $this->_index_property, str_replace(' ','%20',$this->{$this->_index_property}));
		}
	}
	
}

class Author extends IndexNode
{
	public function setIdByUri($uri) {
		$this->id = BlogFunc::findIdByUri($uri);
	}
	
	public function setNName($name) {
		$this->name = BlogFunc::normalize($name); 
	}
}

class Tag extends IndexNode
{
	
}

?>