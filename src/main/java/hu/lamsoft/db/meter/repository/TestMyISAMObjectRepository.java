package hu.lamsoft.db.meter.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import hu.lamsoft.db.meter.entity.TestMyISAMObject;

@Repository
public interface TestMyISAMObjectRepository extends JpaRepository<TestMyISAMObject, Long>{

}
